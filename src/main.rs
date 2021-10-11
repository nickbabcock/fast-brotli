use fastly::http::{header, Method, StatusCode};
use fastly::{Error, Request, Response};
use std::io::{Cursor, Write};

fn main() -> Result<(), Error> {
    let req = Request::from_client();
    if req.get_method() != Method::POST {
        let resp = Response::from_status(StatusCode::METHOD_NOT_ALLOWED)
            .with_header(header::ALLOW, "POST")
            .with_body_text_plain("This method is not allowed\n");

        resp.send_to_client();
        Ok(())
    } else {
        let mut resp = Response::new()
            .with_status(200)
            .with_header("Content-Encoding", "br");

        if let Some(content_type) = req.get_header_str("Content-Type") {
            resp.set_header("Content-Type", content_type);
        }

        let mut streamer = resp.stream_to_client();
        recompress(req, &mut streamer)?;
        drop(streamer);
        Ok(())
    }
}

fn new_brotli<W: Write>(writer: W) -> brotli::CompressorWriter<W> {
    brotli::CompressorWriter::new(writer, 4096, 9, 22)
}

fn recompress<W: Write>(mut req: Request, writer: W) -> Result<(), Error> {
    // Consume the body all at once. It is theoretically possible to process a
    // zip by streaming it, as ["most of the file metadata is duplicated
    // between both the 'central directory record' trailer and a header in
    // front of each file."][0] However, the rust zip library only acclimates
    // itself with the central directory record at the end of the file.
    //
    // What's even worse is that we can't take advantage of reading the first
    // few bytes to detect zip's magic number, as it is valid for there to be
    // an uncompressed header of an arbitrary length that prefixes the magic
    // number.
    //
    // [0]: https://news.ycombinator.com/item?id=28577855
    let upload_data = req.take_body().into_bytes();
    let reader = Cursor::new(&upload_data);
    if let Ok(mut zip) = zip::ZipArchive::new(reader) {
        let compressor = new_brotli(writer);
        let mut archive = tar::Builder::new(compressor);

        for index in 0..zip.len() {
            let file = zip.by_index(index)?;
            let mut header = tar::Header::new_gnu();
            header.set_path(file.name())?;
            header.set_size(file.size());
            header.set_mtime(0);
            header.set_cksum();
            archive.append(&header, file)?;
        }

        archive.finish()?;
        Ok(())
    } else {
        let mut reader = Cursor::new(upload_data);
        let mut compressor = new_brotli(writer);
        std::io::copy(&mut reader, &mut compressor)?;
        Ok(())
    }
}
