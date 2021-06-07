package main

import (
	"fmt"
	"sort"
	"strconv"
	"strings"
	"sync"
)

var numOfTH int = 6

func ExecutePipeline(freeFlowJobs ...job) {
	wg := &sync.WaitGroup{}
	in := make(chan interface{})
	for _, J := range freeFlowJobs {
		wg.Add(1)
		out := make(chan interface{})
		go func(J job, in, out chan interface{}, wg *sync.WaitGroup) {
			defer wg.Done()
			J(in, out)
			close(out)
		}(J, in, out, wg)
		in = out
	}
	wg.Wait()
}

func SingleHash(in, out chan interface{}) {

	mu := &sync.Mutex{}
	wg := &sync.WaitGroup{}

	for singleIn := range in {
		wg.Add(1)
		go func(in interface{}) {
			defer wg.Done()
			input := strconv.Itoa(in.(int))
			mu.Lock()
			md5 := DataSignerMd5(input)
			mu.Unlock()
			crc32Chan := make(chan string)
			go func() {
				crc32Chan <- DataSignerCrc32(input)
			}()
			crc32md5Chan := make(chan string)
			go func() {
				crc32md5Chan <- DataSignerCrc32(md5)
			}()
			crc32md5 := <-crc32md5Chan
			crc32 := <-crc32Chan
			fmt.Println(input, " SingleHash data ", input)
			fmt.Println(input, " SingleHash md5(data) ", md5)
			fmt.Println(input, " SingleHash crc32(md5(data)) ", crc32md5)
			fmt.Println(input, " SingleHash crc32(data) ", crc32)
			fmt.Println(input, " SingleHash result ", crc32+"~"+crc32md5)
			out <- crc32 + "~" + crc32md5
		}(singleIn)
	}

	wg.Wait()
}

func MultiHash(in, out chan interface{}) {
	wg := &sync.WaitGroup{}
	for fu := range in {
		wg.Add(1)
		go func(in interface{}, out chan interface{}) {
			defer wg.Done()
			wgCrc32 := &sync.WaitGroup{}
			multiHashSum := make([]string, numOfTH)
			for th := 0; th < numOfTH; th++ {
				wgCrc32.Add(1)
				go func(idx int) {
					defer wgCrc32.Done()
					strTH := strconv.Itoa(idx)
					crc32 := DataSignerCrc32(strTH + in.(string))
					multiHashSum[idx] = crc32
					fmt.Println(in.(string), " MultiHash: crc32(th+step1))", idx, " ", crc32)
				}(th)
			}
			wgCrc32.Wait()
			outputStr := strings.Join(multiHashSum, "")
			out <- outputStr
		}(fu, out)
	}
	wg.Wait()
}

func CombineResults(in, out chan interface{}) {
	var resultArray []string
	var resultString string
	for result := range in {
		resultArray = append(resultArray, result.(string))
	}
	sort.Strings(resultArray)
	for idx, result := range resultArray {
		resultString += result
		if idx != len(resultArray)-1 {
			resultString += "_"
		}
	}
	out <- resultString
}
