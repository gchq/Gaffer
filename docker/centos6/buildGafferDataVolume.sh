DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
PROXY_ARGS="--build-arg HTTP_PROXY=$http_proxy --build-arg http_proxy=$http_proxy --build-arg HTTPS_PROXY=$http_proxy --build-arg https_proxy=$http_proxy"

echo "Bulding gaffer data volume"
cd $DIR/gaffer
docker build --rm -t gaffer-docker/centos6:gaffer -f Dockerfile.centos6.gaffer $PROXY_ARGS .
echo "Completed building gaffer data volume"

cd $DIR
