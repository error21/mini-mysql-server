#!/bin/bash
# Seed data for Redis

REDIS_CLI="docker compose exec -T redis redis-cli"

echo "Seeding users table..."

# users.{id} -> JSON {name, email, age, created_at}
$REDIS_CLI SET "users.u001" '{"name":"Alice","email":"alice@example.com","age":28,"created_at":"2024-01-15 10:30:00"}'
$REDIS_CLI SET "users.u002" '{"name":"Bob","email":"bob@example.com","age":35,"created_at":"2024-02-20 14:45:00"}'
$REDIS_CLI SET "users.u003" '{"name":"Charlie","email":"charlie@example.com","age":22,"created_at":"2024-03-10 09:15:00"}'
$REDIS_CLI SET "users.u004" '{"name":"Diana","email":"diana@example.com","age":31,"created_at":"2024-04-05 16:20:00"}'
$REDIS_CLI SET "users.u005" '{"name":"Eve","email":"eve@example.com","age":null,"created_at":"2024-05-12 11:00:00"}'

echo "Done. Verifying..."
$REDIS_CLI KEYS "users.*"
