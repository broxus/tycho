use axum::extract::Request;

pub fn get_mime_type(req: &Request) -> Option<&str> {
    req.headers()
        .get(axum::http::header::CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
}

pub const APPLICATION_JSON: &str = "application/json";
pub const APPLICATION_PROTOBUF: &str = "application/x-protobuf";
