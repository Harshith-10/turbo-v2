@echo off
REM Distributed Code Execution System - Docker Image Setup
REM Pulls and configures all required Docker images for the worker node

echo ==========================================
echo   Docker Image Setup for Code Execution
echo ==========================================
echo.

echo Pulling Docker images...
echo.

REM Compilation images
echo Pulling: gcc:latest (C/C++ compilation)
docker pull gcc:latest
echo.

echo Pulling: rust:latest (Rust compilation)
docker pull rust:latest
echo.

echo Pulling: golang:latest (Go compilation)
docker pull golang:latest
echo.

echo Pulling: eclipse-temurin:25 (Java compilation and execution)
docker pull eclipse-temurin:25
echo.

REM Execution images
echo Pulling: python:3-slim (Python execution)
docker pull python:3-slim
echo.

echo Pulling: node:slim (JavaScript execution)
docker pull node:slim
echo.

echo Pulling: ruby:slim (Ruby execution)
docker pull ruby:slim
echo.

echo Pulling: debian:bookworm-slim (Binary execution)
docker pull debian:bookworm-slim
echo.

echo ==========================================
echo   Setup Complete!
echo ==========================================
echo.
echo Installed images:
echo   - gcc:latest           (C/C++ compilation)
echo   - rust:latest          (Rust compilation)
echo   - golang:latest        (Go compilation)
echo   - eclipse-temurin:25      (Java compilation and execution)
echo   - python:3-slim        (Python execution)
echo   - node:slim            (JavaScript execution)
echo   - ruby:slim            (Ruby execution)
echo   - debian:bookworm-slim (Binary execution)
echo.
echo You can now start the worker with: cargo run --bin worker
pause
