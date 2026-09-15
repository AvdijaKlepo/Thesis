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

    let path = handler.resolve("/assets/roundrobin.png").unwrap();

    assert_eq!(path, PathBuf::from("assets/roundrobin.png"));
}

#[test]
fn parent_directory_is_rejected() {
    let handler = StaticFileHandler::new("web");

    assert!(handler.resolve("/../hello.html").is_none());
}

#[test]
fn nested_parent_directory_is_rejected() {
    let handler = StaticFileHandler::new("web");

    assert!(handler.resolve("/assets/../hello.html").is_none());
}

#[test]
fn unknown_path_is_not_rejected_as_invalid() {
    let handler = StaticFileHandler::new("web");

    let path = handler.resolve("/does-not-exist.html").unwrap();

    assert_eq!(path, PathBuf::from("does-not-exist.html"));
}
