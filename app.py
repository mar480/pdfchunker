import csv
import io
import re
import zipfile
from pathlib import PurePosixPath

import streamlit as st
from pypdf import PdfReader, PdfWriter


st.set_page_config(page_title="PDF Chunker", page_icon="📄", layout="centered")


# ----------------------------
# Helpers
# ----------------------------

def make_zip_safe_component(name: str) -> str:
    """
    Convert a filename into something safe to use as a single ZIP path component.

    We keep it as close as possible to the original, but replace characters that
    would break paths or behave unpredictably in ZIP viewers.
    """
    # ZIP path separators must not appear inside a path component
    safe = name.replace("/", "_").replace("\\", "_")

    # Remove control characters
    safe = re.sub(r"[\x00-\x1f\x7f]", "_", safe)

    # Avoid empty / dot-only names
    safe = safe.strip()
    if not safe or safe in {".", ".."}:
        safe = "unnamed.pdf"

    return safe


def deduplicate_folder_name(base_name: str, seen: dict[str, int]) -> str:
    """
    Return a unique folder name:
    example.pdf
    example.pdf (2)
    example.pdf (3)
    """
    count = seen.get(base_name, 0) + 1
    seen[base_name] = count
    if count == 1:
        return base_name
    return f"{base_name} ({count})"


def get_stem(filename: str) -> str:
    if filename.lower().endswith(".pdf"):
        return filename[:-4]
    return filename


def try_read_pdf(uploaded_file):
    """
    Validate PDF and return:
    (pdf_bytes, reader, total_pages, error_message)

    Notes:
    - We reject encrypted PDFs in this MVP.
    - We read bytes once and build PdfReader from memory.
    """
    try:
        pdf_bytes = uploaded_file.getvalue()
        if not pdf_bytes:
            return None, None, None, "File is empty."

        reader = PdfReader(io.BytesIO(pdf_bytes))

        # Reject encrypted files for MVP simplicity
        if reader.is_encrypted:
            return None, None, None, "PDF is encrypted and cannot be processed in this MVP."

        total_pages = len(reader.pages)
        if total_pages == 0:
            return None, None, None, "PDF contains zero pages."

        return pdf_bytes, reader, total_pages, None

    except Exception as e:
        return None, None, None, f"Unreadable PDF: {e}"


def split_reader_into_parts(reader: PdfReader, original_name: str, chunk_size: int):
    """
    Split one PdfReader into chunked PDFs.

    Returns list of parts:
    {
        "part_number": int,
        "start_page": int,
        "end_page": int,
        "filename": str,
        "bytes": bytes
    }
    """
    total_pages = len(reader.pages)
    parts = []
    stem = get_stem(original_name)

    part_number = 1
    for start_idx in range(0, total_pages, chunk_size):
        end_idx = min(start_idx + chunk_size, total_pages)

        writer = PdfWriter()
        for page_idx in range(start_idx, end_idx):
            writer.add_page(reader.pages[page_idx])

        output_buffer = io.BytesIO()
        writer.write(output_buffer)
        output_buffer.seek(0)

        start_page = start_idx + 1
        end_page = end_idx

        part_filename = (
            f"{stem}_part_{part_number:03d}_pages_{start_page}_{end_page}.pdf"
        )

        parts.append(
            {
                "part_number": part_number,
                "start_page": start_page,
                "end_page": end_page,
                "filename": part_filename,
                "bytes": output_buffer.getvalue(),
                "page_count": end_page - start_page + 1,
            }
        )
        part_number += 1

    return parts


def build_manifest_csv(processed_results, failed_results, chunk_size: int) -> bytes:
    """
    Create a manifest CSV summarising all outputs.
    """
    output = io.StringIO()
    writer = csv.writer(output)

    writer.writerow(
        [
            "status",
            "original_filename",
            "zip_folder_name",
            "total_pages",
            "chunk_size",
            "segment_count",
            "segment_filename",
            "part_number",
            "start_page",
            "end_page",
            "segment_page_count",
            "error",
        ]
    )

    for result in processed_results:
        for part in result["parts"]:
            writer.writerow(
                [
                    "processed",
                    result["original_name"],
                    result["zip_folder_name"],
                    result["total_pages"],
                    chunk_size,
                    len(result["parts"]),
                    part["filename"],
                    part["part_number"],
                    part["start_page"],
                    part["end_page"],
                    part["page_count"],
                    "",
                ]
            )

    for failure in failed_results:
        writer.writerow(
            [
                "failed",
                failure["original_name"],
                "",
                "",
                chunk_size,
                "",
                "",
                "",
                "",
                "",
                "",
                failure["error"],
            ]
        )

    return output.getvalue().encode("utf-8")


def build_zip(processed_results, failed_results, chunk_size: int) -> io.BytesIO:
    """
    Build ZIP with structure:

    <zip_folder_name>/segments/<chunk files>
    manifest.csv
    failed_files.csv
    """
    zip_buffer = io.BytesIO()

    manifest_csv = build_manifest_csv(processed_results, failed_results, chunk_size)

    with zipfile.ZipFile(zip_buffer, mode="w", compression=zipfile.ZIP_DEFLATED) as zf:
        # Main manifest
        zf.writestr("manifest.csv", manifest_csv)

        # Chunked PDFs
        for result in processed_results:
            folder = result["zip_folder_name"]
            for part in result["parts"]:
                zip_path = PurePosixPath(folder) / "segments" / part["filename"]
                zf.writestr(str(zip_path), part["bytes"])

    zip_buffer.seek(0)
    return zip_buffer


# ----------------------------
# UI
# ----------------------------

st.title("PDF Chunker")
st.write(
    "Upload one or more PDFs, split them into page chunks, and download the results as a ZIP."
)

chunk_size = st.number_input(
    "Chunk size (pages)",
    min_value=1,
    max_value=500,
    value=70,
    step=1,
    help="Default is 70 pages, but you can change it.",
)

uploaded_files = st.file_uploader(
    "Upload PDF files",
    type=["pdf"],
    accept_multiple_files=True,
)

if uploaded_files:
    st.subheader("Uploaded files")
    for f in uploaded_files:
        st.write(f"- {f.name}")

    if st.button("Split PDFs and build ZIP", type="primary"):
        processed_results = []
        failed_results = []

        progress_bar = st.progress(0, text="Starting...")
        status_box = st.empty()

        folder_name_seen = {}

        total_files = len(uploaded_files)

        for idx, uploaded_file in enumerate(uploaded_files, start=1):
            status_box.write(f"Processing {idx}/{total_files}: **{uploaded_file.name}**")

            pdf_bytes, reader, total_pages, error = try_read_pdf(uploaded_file)

            if error:
                failed_results.append(
                    {
                        "original_name": uploaded_file.name,
                        "error": error,
                    }
                )
            else:
                safe_base_folder = make_zip_safe_component(uploaded_file.name)
                zip_folder_name = deduplicate_folder_name(safe_base_folder, folder_name_seen)

                try:
                    parts = split_reader_into_parts(
                        reader=reader,
                        original_name=uploaded_file.name,
                        chunk_size=int(chunk_size),
                    )

                    processed_results.append(
                        {
                            "original_name": uploaded_file.name,
                            "zip_folder_name": zip_folder_name,
                            "total_pages": total_pages,
                            "parts": parts,
                        }
                    )
                except Exception as e:
                    failed_results.append(
                        {
                            "original_name": uploaded_file.name,
                            "error": f"Failed during splitting: {e}",
                        }
                    )

            progress_bar.progress(
                idx / total_files,
                text=f"Processed {idx} of {total_files} file(s)",
            )

        status_box.write("Finished.")

        # Summary
        st.subheader("Summary")

        if processed_results:
            st.success(f"Processed {len(processed_results)} file(s) successfully.")
            for result in processed_results:
                st.write(
                    f"**{result['original_name']}** — "
                    f"{result['total_pages']} pages → {len(result['parts'])} segment(s)"
                )

                if result["zip_folder_name"] != result["original_name"]:
                    st.caption(
                        f"ZIP folder name used: {result['zip_folder_name']} "
                        f"(original preserved in manifest)"
                    )

                with st.expander(f"View segments for {result['original_name']}"):
                    for part in result["parts"]:
                        st.write(
                            f"- {part['filename']} "
                            f"(pages {part['start_page']}–{part['end_page']})"
                        )

        if failed_results:
            st.warning(f"{len(failed_results)} file(s) could not be processed.")
            with st.expander("View failed files"):
                for failure in failed_results:
                    st.write(f"- **{failure['original_name']}**: {failure['error']}")

        # Build ZIP if anything succeeded
        if processed_results:
            zip_buffer = build_zip(
                processed_results=processed_results,
                failed_results=failed_results,
                chunk_size=int(chunk_size),
            )

            st.download_button(
                label="Download ZIP",
                data=zip_buffer,
                file_name="split_pdfs.zip",
                mime="application/zip",
            )
        else:
            st.error("No ZIP was created because no PDFs were processed successfully.")