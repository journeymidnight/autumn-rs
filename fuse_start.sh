nohup setsid ./target/release/autumn-fuse \
     --manager 127.0.0.1:9001 \
     --mountpoint /mnt/dongmao-share \
     --transport tcp \
     > /var/lib/autumn-rs/logs/fuse.log 2>&1 &
