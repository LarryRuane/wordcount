package main

// This is an example of map-reduce using Go. The map-reduce program
// structure produces a high degree of parallelism.
//
// This program prints the most common numWords (default 10) words, and the
// number of times each occurred, within a tree of text files.
//
// This happens in four phases that run concurrently:
//
// The main thread creates several mapper and reducer threads
// (goroutines). It recursively walks the argument directory, searching
// for files with names ending in .txt. It opens each one and sends its
// file descriptor to a channel.
//
// The mapper goroutines read from this channel, and for each file, the
// mapper reads and parses out its words, hashes each word, and sends
// the word to the corresponding reducer. Since the hashing function is
// random, the reducers will all do about the same amount of work.
//
// An important part of the design is that the hash ensures that each
// reducer receives a unique set of words, so that no two reducers are
// counting the same word separately.
//
// Each reducer reads its stream of words, and enters each unique word
// into a map, also counting the number of occurrences (hence it is
// reducing the amount of data). When it is done (its channel closes),
// it enters all its map entries into a heap (priority queue), then
// pulls out the words with the highest counts, and sends them (along
// with their counts) via a channel back to the main thread.
//
// The main thread enters each word into another instance of the same
// word-counting heap, but this heap will be fairly small (only
// numWords times the number of reducers). Finally, it pulls out
// the most common numWords words from this heap and prints them.
//
// The map-reduce structure of this program can easily be scaled out to
// a cluster of machines on a network, where channels are replaced by
// network connections, enabling massively large problem sets. (L Ruane)

import (
	"bufio"
	"container/heap"
	"fmt"
	"hash/crc32"
	"os"
	"strconv"
	"strings"
)

// These goroutine counts can be set arbitrarily (space-time tradeoff).
const nmappers = 12
const nreducers = 8

// records the number of occurrences of a particular word
type wc struct {
	word  string
	count int
}

// This heap (priority queue) structure is used by the reducers
// to send their most common words to back to the main thread.
type wcHeap []*wc

func (wch wcHeap) Len() int { return len(wch) }
func (wch wcHeap) Less(i, j int) bool {
	// we want the *most* commonly occurring words (largest counts)
	return wch[i].count > wch[j].count
}
func (wch wcHeap) Swap(i, j int) {
	wch[i], wch[j] = wch[j], wch[i]
}
func (wch *wcHeap) Push(x interface{}) {
	*wch = append(*wch, x.(*wc))
}
func (wch *wcHeap) Pop() interface{} {
	old := *wch
	n := len(old)
	wcItem := old[n-1]
	*wch = old[0 : n-1]
	return wcItem
}

// End heap definitions

// Read the (already-opened) directory, open each item,
// send the non-director to the mappers, recurse on
// the directories.
func walkRecursive(files chan *os.File, fname string) {
	file, err := os.Open(fname)
	if err != nil {
		fmt.Println("bad", err)
		os.Exit(1)
	}
	if strings.HasSuffix(fname, ".txt") {
		files <- file
		return
	}
	dirContents, err := file.Readdirnames(0)
	if err != nil {
		// this is a non-txt file, non-directory, ignore
		file.Close()
		return
	}
	// fname is a directory, its contents are in 'dirContents'
	{
		err := os.Chdir(fname)
		if err != nil {
			fmt.Println("bad", err)
		}
	}
	for _, name := range dirContents {
		walkRecursive(files, name)
	}
	os.Chdir("..")
	file.Close()
}

func walk(files chan *os.File, dir string) {
	walkRecursive(files, dir)
	close(files)
}

func commonWords(startDir string, numWords int) []wc {
	// for hashing the words
	crc32q := crc32.MakeTable(0xD5828281)

	// each reducer will send its most common words to here
	results := make(chan *wc)

	// start recursively reading directories and
	// writing file descriptors to channel 'files'
	files := make(chan *os.File, 4)
	go walk(files, startDir)

	// *** Reducer threads:
	var reduceCh [nreducers]chan string
	for i := range reduceCh {
		noDups := make(map[string]int)
		reduceCh[i] = make(chan string, 8)
		go func(i int) {
			for {
				word, ok := <-reduceCh[i]
				if !ok {
					break
				}
				// creates the map entry (initializes count to 0) if needed
				noDups[word]++
			}
			wch := make(wcHeap, 0)
			heap.Init(&wch)
			for word, count := range noDups {
				wcItem := &wc{word, count}
				heap.Push(&wch, wcItem)
			}
			for i := 0; i < numWords && wch.Len() > 0; i++ {
				wcItem := heap.Pop(&wch).(*wc)
				results <- wcItem
			}
			// this tells the receiving thread that we're done
			results <- &wc{"", 0}
		}(i)
	}

	// *** Mapper threads:
	mappersdone := make(chan struct{})
	for i := 0; i < nmappers; i++ {
		go func(i int) {
			for { // each file
				file, ok := <-files
				if !ok {
					// no more files to process
					break
				}
				input := bufio.NewScanner(file)
				input.Split(bufio.ScanWords)
				for input.Scan() { // each word
					// send each word to a pseudo-random reducer,
					// duplicate words go to the same reducer
					text := input.Text()
					ri := crc32.Checksum([]byte(text), crc32q) % nreducers
					reduceCh[ri] <- text
				}
				file.Close()
			}
			mappersdone <- struct{}{}
		}(i)
	}

	// wait for all the mappers to finish
	for i := 0; i < nmappers; i++ {
		<-mappersdone
	}
	// since the mappers are all done, we can close their
	// pipes to the reducers, so they can finish
	for i := 0; i < nreducers; i++ {
		close(reduceCh[i])
	}
	// read the results from the reducers, put into one final heap
	wch := make(wcHeap, 0)
	heap.Init(&wch)
	remaining := nreducers
	for {
		wcp := <-results
		if wcp.count == 0 {
			// one of the reducers is done
			remaining--
			if remaining == 0 {
				// all the reducers are done
				break
			}
			continue
		}
		heap.Push(&wch, wcp)
	}
	// last step: create a slice of the most common words overall
	most := make([]wc, 0)
	for i := 0; i < numWords && wch.Len() > 0; i++ {
		wcItem := heap.Pop(&wch).(*wc)
		most = append(most, *wcItem)
	}
	return most
}

func main() {
	if len(os.Args) < 2 || len(os.Args) > 3 {
		fmt.Println("usage:", os.Args[0], "directory [numWords]")
		os.Exit(1)
	}
	numWords := 10
	if len(os.Args) == 3 {
		var err error
		numWords, err = strconv.Atoi(os.Args[2])
		if err != nil {
			fmt.Println("bad", err)
			os.Exit(1)
		}
	}
	wcList := commonWords(os.Args[1], numWords)
	for _, w := range wcList {
		fmt.Printf("%10d %s\n", w.count, w.word)
	}
}
