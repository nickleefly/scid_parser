FROM postgres:16

LABEL name="index-postgresql"
LABEL description="PostgreSQL database for Sierra Chart tick data (ES, NQ futures)"

# Set environment variables
ENV POSTGRES_DB=future_index
ENV POSTGRES_USER=postgres
ENV POSTGRES_PASSWORD=postgres

# Copy initialization script
COPY init.sql /docker-entrypoint-initdb.d/

# Expose PostgreSQL port
EXPOSE 5432

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
    CMD pg_isready -U postgres -d future_index || exit 1
