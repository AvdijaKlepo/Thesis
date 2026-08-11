use std::{fs, io, path::{Path, PathBuf}};



pub struct StaticFileHandler {
    root: PathBuf
}

impl StaticFileHandler {
    pub fn new(root: impl Into<PathBuf>) -> Self {
        Self { root: root.into() }
    }

    pub fn serve(&self, request_path: &str) -> io::Result<Response> {
        let (path, content_type) = self.resolve(request_path);

        let file_path = self.root.join(path);

        match fs::read(&file_path) {
            Ok(contents) => Ok(Response::ok(content_type, contents)),
            Err(error) if error.kind() ==io::ErrorKind::NotFound => {
                let contents = fs::read(self.root.join("404.html"))?;

                Ok(Response::not_found(
                    "text/html; charset=utf-8",
                    contents
                ))
            }
            Err(error) => Err(error)
        }
    }

    fn resolve(&self, request_path: &str) -> (PathBuf, &'static str) {
        match request_path {
            "/" => (
                PathBuf::from("hello.html"),
                "text/html; chartset=utf-8"
            ),

            path if path.starts_with("/assets") => {
                let asset_path = &path["/assets/".len()..];

                (
                    PathBuf::from("assets").join(asset_path),
                    content_type(asset_path)
                )
            }

            _ => (
                PathBuf::from("404.html"),
                "text/html; charset=utf-8"
            )
        }
    }


}

pub struct Response {
    status: &'static str,
    content_type : &'static str,
    body: Vec<u8>
}

impl Response {
    fn ok(content_type: &'static str, body: Vec<u8>) -> Self {
        Self { status: "200 OK", content_type, body }
    }

    fn not_found(content_type: &'static str, body: Vec<u8>) -> Self {
        Self { status: "404 NOT FOUND", content_type, body }
    }

    pub fn to_http(&self) -> Vec<u8> {
        let headers = format!(
            "HTTP/1.1 {}\r\n\
             Content-Type: {}\r\n\
             Content-Length: {}\r\n\
             Connection: close\r\n\
             \r\n",
            self.status,
            self.content_type,
            self.body.len(),
        );

        let mut response = Vec::with_capacity(
            headers.len() + self.body.len()
        );

        response.extend_from_slice(headers.as_bytes());
        response.extend_from_slice(&self.body);

        response
    }
}

fn content_type(path: &str) -> &'static str {
    match Path::new(path)
    .extension()
    .and_then(|extension| extension.to_str())
    {
        Some("html") => "text/html; charset=utf-8",
        Some("css") => "text/css; charset=utf-8",
        Some("js") => "application/javascript",
        Some("png") => "image/png",
        Some("jpg") | Some("jpeg") => "image/jpeg",
        Some("svg") => "image/svg+xml",
        Some("ico") => "image/x-icon",
        _ => "application/octet-stream",
    }
}
