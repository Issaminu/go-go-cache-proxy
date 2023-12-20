package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/go-redis/redis"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"log"
	"net/http"
	"strconv"
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
	val, err := getFromRedis(id)
	var result map[string]interface{}
	if err != nil {
		// Cache miss from redis
		println("==== CACHE MISS ON post-" + id + " ====")
		res, err := getFromMongo(id)
		if errors.Is(err, mongo.ErrNoDocuments) {
			// DB miss from mongodb
			println("==== DB MISS ON post-" + id + " ====")
			url := "https://jsonplaceholder.typicode.com/posts/" + id
			response, err := http.Get(url)
			err = json.NewDecoder(response.Body).Decode(&result)
			if err != nil {
				fmt.Println("Error fetching data:", err)
				return
			}
			_ = saveToMongo(id, result)
		} else {
			// DB hit from mongodb
			println("==== DB HIT ON post-" + id + " ====")
			result = res
		}

		_ = saveToRedis("post-"+id, result)
	} else {
		// Cache hit from redis
		println("==== CACHE HIT ON post-" + id + " ====")
		err := json.Unmarshal([]byte(val), &result)
		if err != nil {
			fmt.Println("Error decoding JSON:", err)
			return
		}
	}
	elapsedTime := time.Since(startTime).Seconds() * 1000
	fmt.Printf("Execution time: %.5f milliseconds\n", elapsedTime)
	resultStr, _ := mapToString(result)
	w.Write([]byte(resultStr))
}

func saveToMongo(id string, doc bson.M) error {

	// Set up MongoDB connection options
	clientOptions := options.Client().ApplyURI("mongodb://localhost:27017")
	// Connect to MongoDB
	client, err := mongo.Connect(context.TODO(), clientOptions)
	if err != nil {
		panic(err)
	}

	// Access the "users" collection
	collection := client.Database("local").Collection("posts")

	idInt, _ := strconv.Atoi(id)

	filter := bson.D{{"id", idInt}}

	existingDoc := collection.FindOne(context.Background(), filter)
	// Check if the document exists
	if existingDoc.Err() == nil {
		fmt.Println("Document already exists.")
		return fmt.Errorf("document already exists")
	}

	_, err = collection.InsertOne(context.Background(), doc)
	if err != nil {
		log.Fatal(err)
	}
	return nil
}

func getFromMongo(id string) (bson.M, error) {
	clientOptions := options.Client().ApplyURI("mongodb://localhost:27017")
	client, err := mongo.Connect(context.TODO(), clientOptions)
	if err != nil {
		panic(err)
	}

	// Check the connection
	err = client.Ping(context.TODO(), nil)
	if err != nil {
		return nil, fmt.Errorf("can't connect to db")
	}

	collection := client.Database("local").Collection("posts")

	idInt, _ := strconv.Atoi(id)

	filter := bson.D{{"id", idInt}}
	var result bson.M
	err = collection.FindOne(context.TODO(), filter).Decode(&result)
	if err != nil {
		if errors.Is(err, mongo.ErrNoDocuments) {
			return nil, err
		} else {
			panic(err)
		}
	}

	// Close the MongoDB connection
	defer client.Disconnect(context.TODO())

	return result, nil
}

func saveToRedis(key string, val bson.M) error {
	client := getRedisClient()
	_, err := client.Ping().Result()
	if err != nil {
		fmt.Println("Error connecting to Redis:", err)
		return fmt.Errorf("can't connect to redis")
	}
	value, err := convertBsonMtoJSON(val)
	if err != nil {
		return fmt.Errorf("can't convert bson to json")
	}
	strVal, _ := mapToString(value)
	err = client.Set(key, strVal, 5000000000).Err() // Expiration is in nanoseconds, current value is 5 seconds
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

func convertBsonMtoJSON(bsonMap bson.M) (map[string]interface{}, error) {
	jsonMap := make(map[string]interface{})

	for key, value := range bsonMap {
		jsonKey := fmt.Sprintf("%v", key)
		jsonMap[jsonKey] = value
	}

	return jsonMap, nil
}

func printBsonM(bsonMap bson.M) {
	println("Printing BSON Map:")
	for key, value := range bsonMap {
		fmt.Printf("%s: %v\n", key, value)
	}
}

func printMap(inputMap map[string]interface{}) {
	println("Printing Map:")
	for key, value := range inputMap {
		fmt.Printf("%s: %v\n", key, value)
	}
}
