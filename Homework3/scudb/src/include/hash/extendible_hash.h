/*
 * extendible_hash.h : implementation of in-memory hash table using extendible
 * hashing
 *
 * Functionality: The buffer pool manager must maintain a page table to be able
 * to quickly map a PageId to its corresponding memory location; or alternately
 * report that the PageId does not match any currently-buffered page.
 */

#pragma once

#include <cstdlib>
#include <vector>
#include <string>
#include <mutex>
#include <map>
#include <memory>

#include "hash/hash_table.h"

using namespace std;

namespace scudb {

    template<typename K, typename V>
    class ExtendibleHash : public HashTable<K, V>
    {
        struct Bucket
        {
            Bucket(int depth) : localDepth(depth) {};
            // used to store hash key and value
            map<K, V> kmap;
            // thread safety
            mutex latch;
            // local depth of the bucket
            int localDepth;
        };

    public:
        // constructor
        ExtendibleHash(size_t size);
        // helper function to generate hash addressing
        size_t HashKey(const K &key) const;
        // helper function to get global & local depth
        int GetGlobalDepth() const;
        int GetLocalDepth(int bucket_id) const;
        int GetNumBuckets() const;
        // lookup and modifier
        bool Find(const K &key, V &value) override;
        bool Remove(const K &key) override;
        void Insert(const K &key, const V &value) override;

    private:
        // add your own member variables here
        int getIdx(const K &key) const;
        int globalDepth;
        size_t bucketSize;
        int bucketNum;
        vector<shared_ptr<Bucket>> directories;
        mutable mutex latch;
    };
} // namespace scudb