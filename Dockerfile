FROM gcc:14-bookworm@sha256:5e927c284bf55a7dc796262e311a0703344f62f41f5621eb56843111b1d37e15 AS sqlite-build
WORKDIR /tmp/sqlite-build
RUN curl --proto '=https' --tlsv1.2 --fail --show-error --silent --location --max-time 60 https://www.sqlite.org/2026/sqlite-autoconf-3540100.tar.gz -o sqlite.tar.gz \
 && printf '926ef8d3eab93fbf6e06011901273eec076b25594a4e1cdadd50c987f4f6d64e  sqlite.tar.gz\n' | sha256sum -c - \
 && tar -xzf sqlite.tar.gz --strip-components=1 \
 && mkdir -p /opt/sqlite/lib \
 && gcc -O2 -fPIC -shared -DSQLITE_THREADSAFE=1 -DSQLITE_ENABLE_FTS5 -DSQLITE_ENABLE_COLUMN_METADATA -DSQLITE_DEFAULT_MEMSTATUS=0 -Wl,-soname,libsqlite3.so.0 sqlite3.c -o /opt/sqlite/lib/libsqlite3.so.0 -lm -ldl -lpthread \
 && ln -s libsqlite3.so.0 /opt/sqlite/lib/libsqlite3.so
FROM python:3.13-slim-bookworm@sha256:281d39ff02e3b389401586134ff9e6572954687822a58055ec07a60ded4e0b7f
COPY --from=sqlite-build /opt/sqlite /opt/sqlite
ENV LD_LIBRARY_PATH=/opt/sqlite/lib
ENV PYTHONDONTWRITEBYTECODE=1
WORKDIR /app
COPY . /app
RUN python3 tools/qualify_sqlite.py
USER 65534:65534
CMD ["python3", "-m", "outbox_lab", "demo"]
