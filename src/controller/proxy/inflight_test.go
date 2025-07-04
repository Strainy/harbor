//  Copyright Project Harbor Authors
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.

package proxy

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestInflightRequest(t *testing.T) {
	artName := "hello-world:latest"
	inflightChecker.addRequest(artName)
	_, ok := inflightChecker.reqMap[artName]
	assert.True(t, ok)
	inflightChecker.removeRequest(artName)
	_, exist := inflightChecker.reqMap[artName]
	assert.False(t, exist)
}

func TestSingleflightBlobDeduplication(t *testing.T) {
	callCount := 0
	artifactKey := "test-repo:test-blob"
	
	var wg sync.WaitGroup
	const numGoroutines = 5
	results := make(chan string, numGoroutines)
	
	// Start multiple goroutines concurrently calling the same key
	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			
			result, _, _ := blobGroup.Do(artifactKey, func() (interface{}, error) {
				callCount++
				time.Sleep(10 * time.Millisecond) // Simulate work
				return fmt.Sprintf("result-%d", callCount), nil
			})
			
			results <- result.(string)
		}(i)
	}
	
	wg.Wait()
	close(results)
	
	// Verify all goroutines got the same result
	firstResult := ""
	for result := range results {
		if firstResult == "" {
			firstResult = result
		} else {
			assert.Equal(t, firstResult, result)
		}
	}
	
	// Only one execution should have occurred
	assert.Equal(t, 1, callCount)
}

func TestSingleflightManifestDeduplication(t *testing.T) {
	callCount := 0
	artifactKey := "test-repo:test-manifest"
	
	var wg sync.WaitGroup
	const numGoroutines = 3
	results := make(chan string, numGoroutines)
	
	// Start multiple goroutines concurrently calling the same key
	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			
			result, _, _ := manifestGroup.Do(artifactKey, func() (interface{}, error) {
				callCount++
				time.Sleep(10 * time.Millisecond) // Simulate work
				return fmt.Sprintf("manifest-%d", callCount), nil
			})
			
			results <- result.(string)
		}(i)
	}
	
	wg.Wait()
	close(results)
	
	// Verify all goroutines got the same result
	firstResult := ""
	for result := range results {
		if firstResult == "" {
			firstResult = result
		} else {
			assert.Equal(t, firstResult, result)
		}
	}
	
	// Only one execution should have occurred
	assert.Equal(t, 1, callCount)
}
