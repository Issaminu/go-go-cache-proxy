package main

import (
	"encoding/json"
	"fmt"
	"github.com/go-redis/redis"
	"net/http"
	"time"
)

func main() {
	http.HandleFunc("/getPost", getPost)

	println("Server running on port 3333")
	err := http.ListenAndServe(":3333", nil)
	if err != nil {
		panic(err)
	}
}

func getPost(w http.ResponseWriter, r *http.Request) {
	startTime := time.Now()
	id := r.URL.Query().Get("id")
	var prettyJSON []byte
	val, err := getFromRedis(id)
	if err != nil {
		// Can't miss val from redis
		println("==== CACHE MISS ON post-" + id + " ====")
		url := "https://jsonplaceholder.typicode.com/posts/" + id
		response, err := http.Get(url)
		var result map[string]interface{}
		err = json.NewDecoder(response.Body).Decode(&result)
		if err != nil {
			fmt.Println("Error fetching data:", err)
			return
		}
		resultStr, _ := mapToString(result)
		_ = saveToRedis("post-"+id, resultStr)
		prettyJSON, _ = prettifyJSON(resultStr)
	} else {
		// Cache hit from redis
		println("==== CACHE HIT ON post-" + id + " ====")
		prettyJSON, _ = prettifyJSON(val)
	}
	elapsedTime := time.Since(startTime).Seconds() * 1000
	fmt.Printf("Execution time: %.5f milliseconds\n", elapsedTime)
	w.Write(prettyJSON)
}

func saveToRedis(key string, val string) error {
	client := getRedisClient()

	_, err := client.Ping().Result()
	if err != nil {
		fmt.Println("Error connecting to Redis:", err)
		return fmt.Errorf("can't connect to redis")
	}

	// Example: Set a key-value pair
	err = client.Set(key, val, 5000000000).Err() // Expiration is in nanoseconds, current value is 5 seconds
	if err != nil {
		return fmt.Errorf("can't set key")
	}
	return nil
}

func getFromRedis(postID string) (string, error) {
	client := getRedisClient()

	_, err := client.Ping().Result()
	if err != nil {
		return "", fmt.Errorf("can't connect to redis")
	}

	val, err := client.Get("post-" + postID).Result()
	if err == redis.Nil {
		return "", fmt.Errorf("not found")
	} else if err != nil {
		fmt.Println("Error getting value:", err)
	} else {
	}
	return val, nil

}

func prettifyJSON(val string) ([]byte, error) {
	prettyJSON, err := json.MarshalIndent(val, "", "  ")
	if err != nil {
		return nil, fmt.Errorf("can't format json")
	}
	return prettyJSON, nil
}

func mapToString(inputMap map[string]interface{}) (string, error) {
	jsonData, err := json.Marshal(inputMap)
	if err != nil {
		return "", err
	}

	return string(jsonData), nil
}

func getRedisClient() *redis.Client {
	return redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: "",
		DB:       0,
	})
}
