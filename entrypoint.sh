
#!/usr/bin/env bash
case "$1" in
*)
  source ~/.bashrc
  exec "$@"
     ;;
esac