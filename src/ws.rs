use crate::events::Event;
use axum::{
    Router,
    extract::{
        State,
        ws::{Message, WebSocket, WebSocketUpgrade},
    },
    response::IntoResponse,
    routing::get,
};
use tokio::sync::broadcast;
use tower_http::services::ServeDir;

pub fn router(events: broadcast::Sender<Event>) -> Router {
    Router::new()
        .route("/ws", get(ws_handler))
        .fallback_service(ServeDir::new("static"))
        .with_state(events)
}

async fn ws_handler(
    ws: WebSocketUpgrade,
    State(events): State<broadcast::Sender<Event>>,
) -> impl IntoResponse {
    ws.on_upgrade(move |socket| handle_socket(socket, events.subscribe()))
}

async fn handle_socket(mut socket: WebSocket, mut rx: broadcast::Receiver<Event>) {
    while let Ok(event) = rx.recv().await {
        let json = serde_json::to_string(&event).unwrap();
        if socket.send(Message::Text(json.into())).await.is_err() {
            break;
        }
    }
}
