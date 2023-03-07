#pragma once

#include <fstream>
#include <vector>
#include <cassert>
#include <sstream>
#include <iostream>

namespace my {
    template<typename _Node>
    class CSR {
    private:
        const long long __max_vertex_number = 5000000;

        _Node vertex_number;
        _Node edge_number;
        _Node *row_ptr;
        _Node *column;

        std::ifstream &build(std::ifstream &infile);
        std::ifstream &restore(std::ifstream &infile);
    public:
        typedef const _Node* edge_iter;

        CSR()=default;
        CSR(std::ifstream &infile, bool binary=false);
        ~CSR();

        std::ofstream &save(std::ofstream &outfile);

        edge_iter begin(_Node n) const {
            assert(n < vertex_number);
            return &column[row_ptr[n]];
        }

        edge_iter end(_Node n) const {
            assert(n < vertex_number);
            return &column[row_ptr[n+1]];
        }

        _Node get_vertex_number() const { return vertex_number; }
        _Node get_edge_number() const { return edge_number; }
        _Node get_outdegree(_Node n) { return row_ptr[n+1]-row_ptr[n]; }
    };
}

template<typename _Node>
my::CSR<_Node>::CSR(std::ifstream &infile, bool binary) {
    if (binary) {
        restore(infile);
    } else {
        build(infile);
    }
}

template<typename _Node>
my::CSR<_Node>::~CSR() {
    if (row_ptr) delete[] row_ptr;
    if (column) delete[] column;
}

template<typename _Node>
std::ifstream &my::CSR<_Node>::build(std::ifstream &infile) {
    infile >> vertex_number >> edge_number;
    infile.ignore();
    edge_number *= 2;
    row_ptr = new _Node[vertex_number+1];
    column = new _Node[edge_number];
    std::string line;
    std::vector<_Node> neighs;
    neighs.reserve(100000);
    _Node r{0}, c{0};
    while (std::getline(infile, line)) {
        std::istringstream iss(line);
        _Node n;
        row_ptr[r++] = c;
        while (iss >> n) {
            column[c++] = n - 1;
        }
    }
    row_ptr[r] = c;
    return infile;
}


template<typename _Node>
std::ifstream &my::CSR<_Node>::restore(std::ifstream &infile) {
    infile.read(reinterpret_cast<char*>(&vertex_number), sizeof(_Node));
    infile.read(reinterpret_cast<char*>(&edge_number), sizeof(_Node));
    row_ptr = new _Node[vertex_number+1];
    column = new _Node[edge_number];
    infile.read(reinterpret_cast<char*>(row_ptr), sizeof(_Node) * (vertex_number + 1));
    infile.read(reinterpret_cast<char*>(column), sizeof(_Node) * edge_number);
    return infile;
}

template<typename _Node>
std::ofstream &my::CSR<_Node>::save(std::ofstream &outfile) {
    outfile.write(reinterpret_cast<char*>(&vertex_number), sizeof(_Node));
    outfile.write(reinterpret_cast<char*>(&edge_number), sizeof(_Node));
    outfile.write(reinterpret_cast<char*>(row_ptr), sizeof(_Node) * (vertex_number + 1));
    outfile.write(reinterpret_cast<char*>(column), sizeof(_Node) * edge_number);
    return outfile;
}
