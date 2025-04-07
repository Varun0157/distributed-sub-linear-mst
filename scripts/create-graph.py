import random
import argparse


def generate_graph(n, m, output_file):
    edges = set()

    while len(edges) < m:
        u = random.randint(1, n)
        v = random.randint(1, n)

        if u != v and (u, v) not in edges:
            edges.add((u, v))

    with open(output_file, "w") as f:
        for u, v in edges:
            weight = random.randint(1, 1000)
            f.write(f"{u} {v} {weight}\n")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Generate a weighted graph.")
    parser.add_argument("n", type=int, help="Number of nodes")
    parser.add_argument("m", type=int, help="Number of edges")
    parser.add_argument("output", type=str, help="Output file name")

    args = parser.parse_args()
    generate_graph(args.n, args.m, args.output)
