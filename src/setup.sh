comms_dir=comms

rm $comms_dir/*.go
protoc --go_out=. --go_opt=paths=source_relative \
  --go-grpc_out=. --go-grpc_opt=paths=source_relative \
  $comms_dir/$comms_dir.proto
