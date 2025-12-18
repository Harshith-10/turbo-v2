//! Common crate for shared gRPC types and utilities
//! 
//! This crate provides the generated protobuf types for the distributed
//! code execution system, including both client and server implementations.

pub mod scheduler {
    tonic::include_proto!("scheduler");
}

// Re-export commonly used types for convenience
pub use scheduler::*;
