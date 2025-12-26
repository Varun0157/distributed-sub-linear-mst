# a temporary testing script

echo "creating a large graph"
python scripts/create-graph.py 5 10 data/graph.txt

echo && echo "getting gt mst details"
python scripts/kruskals.py data/graph.txt

sleep 2

echo && echo "getting distributed mst results"
cd src || exit
go run ./*.go ../data/graph.txt out.txt 0.5
[ -f out.txt ] && rm out.txt
cd - || exit
