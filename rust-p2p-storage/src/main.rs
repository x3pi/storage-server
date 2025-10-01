use axum::{
    extract::{Path, State},
    http::StatusCode,
    routing::{get, post},
    Json, Router,
};
use serde::{Deserialize, Serialize};
use std::net::SocketAddr;
use std::sync::Arc;

// ## CÁC CẤU TRÚC DỮ LIỆU ##

// Struct để nhận payload khi Go Listener gọi /store
#[derive(Deserialize)]
struct StorePayload {
    #[serde(rename = "fileKey")]
    file_key: String,
    #[serde(rename = "chunkHash")]
    chunk_hash: String,
    #[serde(rename = "chunkData")]
    chunk_data: String, // Dữ liệu chunk ở dạng Base64
}

// Struct để trả về khi Go Downloader gọi /file/:fileKey
#[derive(Serialize)]
struct FileChunksResponse {
    #[serde(rename = "fileKey")]
    file_key: String,
    chunks: Vec<Chunk>,
}

// Struct đại diện cho một chunk trong mảng trả về
#[derive(Serialize)]
struct Chunk {
    key: String,   // Key tổng hợp, ví dụ: "0x...:0x..."
    value: String, // Dữ liệu chunk ở dạng Base64
}

// Struct để serialize/deserialize dữ liệu chunk trong database
#[derive(Serialize, Deserialize)]
struct StoredChunkValue {
    value: String,
}


// ## HÀM MAIN - KHỞI TẠO SERVER ##

#[tokio::main]
async fn main() {
    // Mở hoặc tạo database. Dữ liệu sẽ được lưu trong thư mục "my_database"
    let db = sled::open("my_database").expect("Không thể mở database");
    
    // Bọc database trong Arc để chia sẻ an toàn giữa các thread
    let shared_state = Arc::new(db);

    // Định nghĩa các route cho ứng dụng
    let app = Router::new()
        .route("/store", post(store_chunk))
        .route("/file/:fileKey", get(retrieve_file_chunks))
        .with_state(shared_state);

    // Chạy server
    let addr = SocketAddr::from(([127, 0, 0, 1], 3000));
    println!("🚀 Server lưu trữ đang lắng nghe trên http://{}", addr);
    let listener = tokio::net::TcpListener::bind(addr).await.unwrap();
    axum::serve(listener, app).await.unwrap();
}


// ## CÁC HANDLER XỬ LÝ REQUEST ##

/// Handler cho việc LƯU TRỮ chunk mới
async fn store_chunk(
    State(db): State<Arc<sled::Db>>,
    Json(payload): Json<StorePayload>,
) -> StatusCode {
    // Tạo key tổng hợp để lưu vào database, định dạng: "fileKey:chunkHash"
    let db_key = format!("{}:{}", payload.file_key, payload.chunk_hash);

    // Chuẩn bị value để lưu. Chúng ta sẽ lưu lại cấu trúc JSON {"value": "..."}
    let db_value = StoredChunkValue {
        value: payload.chunk_data,
    };

    // Serialize value thành JSON bytes để lưu trữ
    let value_bytes = match serde_json::to_vec(&db_value) {
        Ok(bytes) => bytes,
        Err(e) => {
            eprintln!("Lỗi khi serialize value: {}", e);
            return StatusCode::INTERNAL_SERVER_ERROR;
        }
    };
    
    println!("-> Đang lưu chunk với key: {}", db_key);

    // Lưu cặp key-value vào Sled DB
    match db.insert(db_key.as_bytes(), value_bytes) {
        Ok(_) => {
            // Đảm bảo dữ liệu được ghi xuống đĩa một cách bất đồng bộ
            if db.flush_async().await.is_err() {
                eprintln!("Lỗi khi flush database");
                return StatusCode::INTERNAL_SERVER_ERROR;
            }
            StatusCode::OK
        }
        Err(e) => {
            eprintln!("Lỗi khi insert vào database: {}", e);
            StatusCode::INTERNAL_SERVER_ERROR
        }
    }
}

/// Handler cho việc LẤY TẤT CẢ chunk của một file
async fn retrieve_file_chunks(
    State(db): State<Arc<sled::Db>>,
    Path(file_key): Path<String>,
) -> Result<Json<FileChunksResponse>, StatusCode> {
    
    println!("<- Đang truy vấn tất cả chunk cho fileKey: {}", file_key);
    let mut chunks = Vec::new();
    
    // Tạo prefix để quét database. Thêm dấu ':' để đảm bảo không lấy nhầm
    // fileKey khác có tiền tố tương tự.
    let prefix = format!("{}:", file_key);

    // Quét tất cả các key có tiền tố là `file_key:`
    for result in db.scan_prefix(prefix.as_bytes()) {
        match result {
            Ok((key_bytes, value_bytes)) => {
                // Chuyển đổi key từ bytes sang String
                let key_str = match String::from_utf8(key_bytes.to_vec()) {
                    Ok(s) => s,
                    Err(_) => continue, // Bỏ qua nếu key không phải UTF-8 hợp lệ
                };
                
                // Deserialize value từ JSON bytes
                let stored_value: StoredChunkValue = match serde_json::from_slice(&value_bytes) {
                    Ok(v) => v,
                    Err(_) => continue, // Bỏ qua nếu value không phải JSON hợp lệ
                };

                // Thêm chunk đã tìm thấy vào danh sách
                chunks.push(Chunk {
                    key: key_str,
                    value: stored_value.value,
                });
            }
            Err(_) => {
                // Bỏ qua các key lỗi
                continue;
            }
        }
    }

    println!("   -> Tìm thấy {} chunks", chunks.len());

    // Tạo response cuối cùng
    let response = FileChunksResponse {
        file_key,
        chunks,
    };

    Ok(Json(response))
}