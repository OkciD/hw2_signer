package main

import (
	"fmt"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

func asyncCrc32(data string) chan string {
	resultChan := make(chan string, 1)

	go func(data string, out chan<- string) {
		out <- DataSignerCrc32(data)
	}(data, resultChan)

	return resultChan
}

var quotaChannel = make(chan interface{}, 1)

func asyncMd5(data string) chan string {
	resultChan := make(chan string, 1)

	go func(data string, out chan<- string) {
		quotaChannel <- struct{}{}
		out <- DataSignerMd5(data)
		<-quotaChannel
	}(data, resultChan)

	return resultChan
}

func SingleHash(in, out chan interface{}) {
	wg := &sync.WaitGroup{}

	for data := range in {
		dataString := strconv.FormatInt(int64(data.(int)), 10)

		wg.Add(1)
		go func(data string, out chan<- interface{}, wg *sync.WaitGroup) {
			firstHalf := asyncCrc32(data)
			secondHalf := asyncCrc32(<-asyncMd5(data))

			result := <-firstHalf + "~" + <-secondHalf

			fmt.Println("SingleHash(", data, ") returned", result)
			out <- result

			wg.Done()
		}(dataString, out, wg)
	}

	wg.Wait()
}

func MultiHash(in, out chan interface{}) {
	wg := &sync.WaitGroup{}

	for data := range in {
		wg.Add(1)

		go func(data string, out chan<- interface{}, wg *sync.WaitGroup) {
			var resultChannels []chan string
			for i := 0; i < 6; i++ {
				indexString := strconv.FormatInt(int64(i), 10)
				resultChannels = append(resultChannels, asyncCrc32(indexString+data))
			}

			var result string
			for _, resultChannel := range resultChannels {
				result += <-resultChannel
			}

			fmt.Println("MultiHash(", data, ") returned", result)
			out <- result

			wg.Done()
		}(data.(string), out, wg)
	}

	wg.Wait()
}

func CombineResults(in, out chan interface{}) {
	var results []string

	for data := range in {
		results = append(results, data.(string))
	}

	sort.Strings(results)
	result := strings.Join(results, "_")

	fmt.Println("CombineResults", result)
	out <- result
}

func ExecutePipeline(jobs ...job) {
	const bufferSize int = 1
	in := make(chan interface{}, bufferSize)
	out := make(chan interface{}, bufferSize)
	wg := &sync.WaitGroup{}

	for _, jobFunc := range jobs {
		wg.Add(1)

		go func(jobFunc job, in, out chan interface{}, wg *sync.WaitGroup) {
			jobFunc(in, out)
			close(out)
			wg.Done()
		}(jobFunc, in, out, wg)

		in = out
		out = make(chan interface{}, bufferSize)
	}

	wg.Wait()
}

// todo: remove main
func main() {
	inputData := []int{0, 1, 1, 2, 3, 5, 8}
	//inputData := []int{0, 1}

	hashSignJobs := []job{
		job(func(in, out chan interface{}) {
			for _, fibNum := range inputData {
				out <- fibNum
			}
		}),
		job(SingleHash),
		job(MultiHash),
		job(CombineResults),
	}

	start := time.Now()
	ExecutePipeline(hashSignJobs...)
	end := time.Since(start)

	fmt.Println("Time", end)
}
