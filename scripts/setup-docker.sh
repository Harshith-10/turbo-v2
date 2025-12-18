#!/bin/bash
# Distributed Code Execution System - Docker Image Setup
# Pulls and configures all required Docker images for the worker node

set -e

echo "=========================================="
echo "  Docker Image Setup for Code Execution"
echo "=========================================="
echo ""

# Colors for output
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Required images
IMAGES=(
    # Compilation images
    "gcc:latest"           # C/C++ compilation
    "rust:latest"          # Rust compilation
    "golang:latest"        # Go compilation
    "eclipse-temurin:25"      # Java compilation & execution
    
    # Execution images
    "python:3-slim"        # Python execution
    "node:slim"            # JavaScript/Node.js execution
    "ruby:slim"            # Ruby execution
    "debian:bookworm-slim" # Binary execution (C/C++/Rust/Go)
)

echo -e "${YELLOW}Pulling Docker images...${NC}"
echo ""

for image in "${IMAGES[@]}"; do
    echo -e "${GREEN}Pulling: ${image}${NC}"
    docker pull "$image"
    echo ""
done

echo "=========================================="
echo "  Setup Complete!"
echo "=========================================="
echo ""
echo "Installed images:"
echo "  - gcc:latest           (C/C++ compilation)"
echo "  - rust:latest          (Rust compilation)"
echo "  - golang:latest        (Go compilation)"
echo "  - eclipse-temurin:25      (Java compilation & execution)"
echo "  - python:3-slim        (Python execution)"
echo "  - node:slim            (JavaScript execution)"
echo "  - ruby:slim            (Ruby execution)"
echo "  - debian:bookworm-slim (Binary execution)"
echo ""
echo "You can now start the worker with: cargo run --bin worker"
