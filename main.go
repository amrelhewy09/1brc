package main

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"log"
	"math"
	"net/http"
	_ "net/http/pprof"
	"os"
	"strings"
	"sync"
	"time"
)

type stats struct {
	Min   float64
	Max   float64
	Count float64
	Sum   float64
}

func main() {
	start := time.Now()
	go func() {
		http.ListenAndServe("localhost:6060", nil)
	}()

	file, err := os.Open("measurements.txt")
	if err != nil {
		log.Fatal(err)
	}
	defer file.Close()
	lineWg := sync.WaitGroup{}
	reduceWg := sync.WaitGroup{}
	mainWg := sync.WaitGroup{}
	reduceChan := make(chan map[string]*stats, 150)
	lineChan := make(chan []string, 150)
	chunkChan := make(chan []byte, 20)
	for i := 0; i < 2; i++ {
		lineWg.Add(1)
		go chunkScanner(lineChan, chunkChan, &lineWg)
	}
	for i := 0; i < 3; i++ {
		mainWg.Add(1)
		go reducer(reduceChan, &mainWg)
	}
	for i := 0; i < 3; i++ {
		reduceWg.Add(1)
		go lineConsumer(lineChan, reduceChan, &reduceWg)
	}

	reader := bufio.NewReader(file)
	chunkSize := 1024 * 1024 * 50
	for {
		chunk := make([]byte, chunkSize)
		n, err := io.ReadFull(reader, chunk)
		if err != nil && err != io.EOF && err != io.ErrUnexpectedEOF {
			log.Fatal(err)
		}

		if n < chunkSize || err == io.EOF || err == io.ErrUnexpectedEOF {
			chunkChan <- chunk[:n]
			chunk = nil
			break
		}

		line, err := reader.ReadBytes('\n') // read until '\n' byte

		if err != nil {
			panic(err)
		}
		line = bytes.TrimSuffix(line, []byte{'\n'})
		newChunk := make([]byte, n+len(line))
		copy(newChunk, chunk[:n])
		copy(newChunk[n:], line)

		chunkChan <- newChunk
		chunk = nil
		newChunk = nil
	}

	close(chunkChan)
	go func() {
		lineWg.Wait()
		close(lineChan)
	}()

	go func() {
		reduceWg.Wait()
		close(reduceChan)
	}()

	mainWg.Wait()
	fmt.Printf("Execution time: %v\n", time.Since(start))
}

func parseLine(line string) (string, float64) {
	country, temp := cut(line)
	return country, float64(temp / 10)
}

func lineConsumer(linechan chan []string, reduceChan chan map[string]*stats, wg *sync.WaitGroup) {

	for lines := range linechan {
		myMap := make(map[string]*stats)
		for _, line := range lines {
			country, temp := parseLine(line)
			val := myMap[country]
			if val == nil { // If count is zero, it means the country was not in the map
				val = &stats{
					Min:   temp,
					Max:   temp,
					Sum:   temp,
					Count: 1,
				}
			} else {
				val.Min = math.Min(val.Min, temp)
				val.Max = math.Max(val.Max, temp)
				val.Sum += temp
				val.Count++
			}
			myMap[country] = val
		}
		reduceChan <- myMap
	}
	wg.Done()
}

func reducer(reduceChan chan map[string]*stats, wg *sync.WaitGroup) {
	resMap := make(map[string]*stats)
	for data := range reduceChan {
		for k, v := range data {
			val := resMap[k]
			if val == nil {
				val = v
			} else {
				val.Min = math.Min(v.Min, val.Min)
				val.Max = math.Max(v.Max, val.Max)
				val.Sum = val.Sum + v.Sum
				val.Count = val.Count + v.Count
			}
			resMap[k] = val
		}
	}

	wg.Done()
}

func chunkScanner(lineChan chan []string, chunkChan chan []byte, wg *sync.WaitGroup) {
	for chunk := range chunkChan {
		lines := make([]string, 0, 500)
		scanner := bufio.NewScanner(strings.NewReader(string(chunk)))
		for scanner.Scan() {
			lines = append(lines, scanner.Text())
			if len(lines) == 500 {
				lineChan <- lines
				lines = nil
			}
		}
		if len(lines) > 0 {
			lineChan <- lines
		}
	}
	wg.Done()
}

func cut(line string) (string, int32) {
	end := len(line)
	tenths := int32(line[end-1] - '0')
	ones := int32(line[end-3] - '0') // line[end-2] is '.'
	var temp int32
	var semicolon int
	if line[end-4] == ';' { // positive N.N temperature
		temp = ones*10 + tenths
		semicolon = end - 4
	} else if line[end-4] == '-' { // negative -N.N temperature
		temp = -(ones*10 + tenths)
		semicolon = end - 5
	} else {
		tens := int32(line[end-4] - '0')
		if line[end-5] == ';' { // positive NN.N temperature
			temp = tens*100 + ones*10 + tenths
			semicolon = end - 5
		} else { // negative -NN.N temperature
			temp = -(tens*100 + ones*10 + tenths)
			semicolon = end - 6
		}
	}
	station := line[:semicolon]
	return station, temp
}
