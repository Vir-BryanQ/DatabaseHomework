#include <list>

#include "hash/extendible_hash.h"
#include "page/page.h"

namespace scudb {
/*
 * constructor
 * array_size: fixed array size for each bucket
 */
    template<typename K, typename V>
    ExtendibleHash<K, V>::ExtendibleHash(size_t size):globalDepth(0), bucketSize(size), bucketNum(1)
    {
        directories.push_back(make_shared<Bucket>(0));
    }


/*
 * helper function to calculate the hashing address of input key
 */
    template<typename K, typename V>
    size_t ExtendibleHash<K, V>::HashKey(const K &key) const
    {
        return hash<K>{}(key);
    }

/*
 * helper function to return global depth of hash table
 * NOTE: you must implement this function in order to pass test
 */
    template<typename K, typename V>
    int ExtendibleHash<K, V>::GetGlobalDepth() const
    {
        lock_guard<mutex> lock(latch);// to ensure thread safety
        return globalDepth;
    }

/*
 * helper function to return local depth of one specific bucket
 * NOTE: you must implement this function in order to pass test
 */
    template<typename K, typename V>
    int ExtendibleHash<K, V>::GetLocalDepth(int bucket_id) const
    {
        lock_guard<mutex> lck(directories[bucket_id]->latch);// to ensure thread safety
        return directories[bucket_id]->localDepth;
    }

/*
 * helper function to return current number of bucket in hash table
 */
    template<typename K, typename V>
    int ExtendibleHash<K, V>::GetNumBuckets() const
    {
        lock_guard<mutex> lock(latch);
        return bucketNum;
    }

/*
 * lookup function to find value associate with input key
 */
    template<typename K, typename V>
    bool ExtendibleHash<K, V>::Find(const K &key, V &value)
    {
        int index = getIdx(key);
        lock_guard<mutex> lck(directories[index]->latch);
        if (directories[index]->kmap.find(key) != directories[index]->kmap.end())
        {// return value
            value = directories[index]->kmap[key];
            return true;
        }
        // does not exist
        return false;
    }

/*
 *  helper function to get the index
 */
    template<typename K, typename V>
    int ExtendibleHash<K, V>::getIdx(const K &key) const
    {
        lock_guard<mutex> lock(latch);
        // return globalDepth length LSBs of HashKey(key)
        return HashKey(key) & ((1 << globalDepth) - 1);
    }

/*
 * delete <key,value> entry in hash table
 * Shrink & Combination is not required for this project
 */
    template<typename K, typename V>
    bool ExtendibleHash<K, V>::Remove(const K &key)
    {
        int index = getIdx(key);
        lock_guard<mutex> lck(directories[index]->latch);
        shared_ptr<Bucket> cur = directories[index];// refer to the current bucket
        if (cur->kmap.find(key) == cur->kmap.end())
        {// does not exist
            return false;
        }
        // remove it from the map
        cur->kmap.erase(key);
        return true;
    }

/*
 * insert <key,value> entry in hash table
 * Split & Redistribute bucket when there is overflow and if necessary increase
 * global depth
 */
    template<typename K, typename V>
    void ExtendibleHash<K, V>::Insert(const K &key, const V &value)
    {
        int idx = getIdx(key);
        shared_ptr<Bucket> cur = directories[idx];  // get the specific bucket according to the key
        while (true)
        {
            lock_guard<mutex> lck(cur->latch);
            if (cur->kmap.find(key) != cur->kmap.end() || cur->kmap.size() < bucketSize)
            {
                cur->kmap[key] = value;
                break;
            }
            // from here, deal with the problem about the spliting
            int mask = (1 << (cur->localDepth));  // mask means higher one bit to judge the entry is in old or new bucket.
            cur->localDepth++;

            // out of the scope, the mutex will be released
            // it should be locked when different threads modify the directory
            {
                lock_guard<mutex> lock(latch);  // lock the dictionary
                if (cur->localDepth > globalDepth)
                {
                    size_t len = directories.size();
                    for (size_t i = 0; i < len; i++)
                    {
                        directories.push_back(directories[i]);
                    }
                    globalDepth++;
                }
                bucketNum++;
                auto newBuc = make_shared<Bucket>(cur->localDepth);  // create a new bucket with the new localDepth

                typename map<K, V>::iterator iter = cur->kmap.begin();
                while (iter != cur->kmap.end())
                {  // rehash each entry with a new local depth
                    if (HashKey(iter->first) & mask)
                    {  // if the higher bit is 1, allocate this entry to new bucket, and delete it from old bucket
                        newBuc->kmap[iter->first] = iter->second;
                        iter = cur->kmap.erase(iter);  // erase return the next iter
                    }
                    else
                    {  // else keep it in old bucket
                        iter++;
                    }
                }

                for (size_t i = 0; i < directories.size(); i++)
                {  // assign each directory point to the correct the bucket
                    if (directories[i] == cur && (i & mask))
                    {
                        directories[i] = newBuc;
                    }
                }
            }
            idx = getIdx(key);
            cur = directories[idx];  // the current bucket may still be full
        }
    }

    template
    class ExtendibleHash<page_id_t, Page *>;

    template
    class ExtendibleHash<Page *, std::list<Page *>::iterator>;

// test purpose
    template
    class ExtendibleHash<int, std::string>;

    template
    class ExtendibleHash<int, std::list<int>::iterator>;

    template
    class ExtendibleHash<int, int>;
} // namespace scudb