#pragma once

#include "CSR.h"
#include "Partition.h"
#include "utils/bitmap.h"
#include <queue>
#include <cmath>
#include <fstream>
#include <iostream>

namespace my {
    template<typename _Node, typename _PIdx>
    class ParQ {
    private:
        const int __max_hop = 5;
        const double sigma;
        my::CSR<_Node> &csr;
        my::Partition<_Node, _PIdx> &partition;
    
        double *score;
    public:
        ParQ(my::CSR<_Node> &_csr, my::Partition<_Node, _PIdx> &_partition, double _sigma=0.4);
        ~ParQ();
    
        _PIdx get_partition_number() const { return partition.get_partition_number(); };
        my::Partition<_Node, _PIdx> &get_partition() const { return partition; }
    
        void calculate_scores();
        void set_score(_Node n, double s) { score[n] = s; }
        void set_negative(_Node n) { if (score[n] > 0.0) score[n] = -score[n]; }
    
        std::ofstream &save(std::ofstream &outfile);
    
        void reset();
        const double &operator[](size_t n) const {
            assert(n < csr.get_vertex_number());
            return score[n];
        }

        void show();
    };
}

template<typename _Node, typename _PIdx>
my::ParQ<_Node, _PIdx>::ParQ(my::CSR<_Node> &_csr, my::Partition<_Node, _PIdx> &_partition, double _sigma) 
    : csr{_csr}, partition{_partition}, sigma{_sigma} {
    score = new double[csr.get_vertex_number()];
    std::fill(score, score+csr.get_vertex_number(), 0.0);
}

template<typename _Node, typename _PIdx>
my::ParQ<_Node, _PIdx>::~ParQ() {
    if (score) delete[] score;
}

template<typename _Node, typename _PIdx>
void my::ParQ<_Node, _PIdx>::calculate_scores() {
    #pragma omp parallel for schedule(static, 30)
    for (_Node i = 0; i < csr.get_vertex_number(); ++i) {
        bool is_cross_vertex = false;
        using Iter = typename my::CSR<_Node>::edge_iter;
        for (Iter it = csr.begin(i); it < csr.end(i); ++it) {
            if (partition[i] != partition[*it]) {
                is_cross_vertex = true;
                break;
            }
        }
        if (is_cross_vertex) {
            // using bounded bfs to count hop neighbors
            _Node root = i;
            Bitmap bmp(csr.get_vertex_number());
            bmp.reset();
            std::queue<_Node> frontier;
            frontier.emplace(root);
            bmp.set_bit(root);
            int level = 0; // 0-hop
            double tmp_score = 0;
            while (level <= __max_hop && !frontier.empty()) {
                int level_count = frontier.size();
                tmp_score += std::pow(sigma, level)*level_count; // calculate sigma^k * |H_k|
                while (level_count-- > 0) {
                    _Node n = frontier.front();
                    for (Iter it = csr.begin(n); it < csr.end(n); ++it) {
                        if (partition[*it] == partition[n] && !bmp.get_bit(*it)) {
                            frontier.emplace(*it);
                            bmp.set_bit(*it);
                        }
                    }
                    frontier.pop();
                }
                level++;
            }
            score[i] = tmp_score;
        }
    }
}

template<typename _Node, typename _PIdx>
std::ofstream &my::ParQ<_Node, _PIdx>::save(std::ofstream &outfile) {
    _Node vertex_number = csr.get_vertex_number();
    outfile.write(reinterpret_cast<char*>(&vertex_number), sizeof(_Node));
    outfile.write(reinterpret_cast<char*>(score), sizeof(double)*vertex_number);
    return outfile;
}

template<typename _Node, typename _PIdx>
void my::ParQ<_Node, _PIdx>::reset() {
    _Node vertex_number = csr.get_vertex_number();
    #pragma omp parallel for
    for (_Node i = 0; i < vertex_number; ++i) {
        score[i] = std::abs(score[i]);
    }
}

template<typename _Node, typename _PIdx>
void my::ParQ<_Node, _PIdx>::show() {
    size_t cnt = 0;
    double max_s = 0.0;
    for (int i = 0; i < csr.get_vertex_number(); ++i) {
        if (score[i] - 0.0 > 1e-5) {
            cnt++;
            max_s = std::max(max_s, score[i]);
        }
    }
    std::cout << cnt << " " << max_s << std::endl;
}