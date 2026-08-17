use std::{fs, io, path::{Component, Path, PathBuf}};



pub struct StaticFileHandler {
    root: PathBuf
}

impl StaticFileHandler {
    pub fn new(root: impl Into<PathBuf>) -> Self {
        Self { root: root.into() }
    }


    fn safe_path(&self, request_path: &str) -> Option<PathBuf> {
    let path = request_path.trim_start_matches('/');

    let mut result = PathBuf::new();

    for component in PathBuf::from(path).components() {
        match component {
            Component::Normal(part) => {
                result.push(part);
            }

            Component::CurDir => {}

            Component::ParentDir => {
                return None;
            }

            Component::RootDir | Component::Prefix(_) => {
                return None;
            }
        }
    }
    Some(result)
}

    pub fn serve(&self, request_path: &str) -> io::Result<Response> {
       

        let relative_path = match self.safe_path(request_path) {
            Some(path) => path,
            None => return self.not_found(),
        };

        let file_path = self.root.join(relative_path);

        match fs::read(&file_path) {
            Ok(contents) => {
                let content_type = content_type(
                    file_path.to_string_lossy().as_ref()
                );

                Ok(Response::ok(content_type, contents))
            }

            Err(error) if error.kind() == io::ErrorKind::NotFound => {
                self.not_found()
            }
            Err(error) => Err(error)
        }
    }

    fn resolve(&self, request_path: &str) -> Option<PathBuf> {
        let path = match request_path {
            "/" => "hello.html",

            path if path.starts_with('/') => {
                path.trim_start_matches('/')
            }

            _ => return None,
        };

        self.safe_path(path)
    }

    fn not_found(&self) -> io::Result<Response> {
        let contents = fs::read(self.root.join("404.html"))?;

        Ok(Response::not_found("text/html; charset=utf-8", contents))
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


#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn root_resolves_to_hello_html() {
        let handler = StaticFileHandler::new("web");

        let path = handler.resolve("/").unwrap();

        assert_eq!(path, PathBuf::from("hello.html"));
    }

    #[test]
    fn asset_path_is_resolved() {
        let handler = StaticFileHandler::new("web");

        let path = handler
            .resolve("/assets/roundrobin.png")
            .unwrap();

        assert_eq!(
            path,
            PathBuf::from("assets/roundrobin.png")
        );
    }

    #[test]
    fn parent_directory_is_rejected() {
        let handler = StaticFileHandler::new("web");

        assert!(handler.resolve("/../hello.html").is_none());
    }

    #[test]
    fn nested_parent_directory_is_rejected() {
        let handler = StaticFileHandler::new("web");

        assert!(
            handler
                .resolve("/assets/../hello.html")
                .is_none()
        );
    }

    #[test]
    fn unknown_path_is_not_rejected_as_invalid() {
        let handler = StaticFileHandler::new("web");

        let path = handler
            .resolve("/does-not-exist.html")
            .unwrap();

        assert_eq!(
            path,
            PathBuf::from("does-not-exist.html")
        );
    }
}