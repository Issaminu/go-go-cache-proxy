package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"strconv"
	"time"

	"github.com/go-redis/redis"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

const (
	serverPort      = ":3333"
	redisKey        = "post-"
	mongoURI        = "mongodb://localhost:27017"
	redisAddr       = "localhost:6379"
	redisPassword   = ""
	redisDB         = "local"
	redisCollection = "posts"
	expiration      = 5 * time.Second
)

func main() {
	http.HandleFunc("/getPost", getPost)
	fmt.Printf("Server running on port %s\n", serverPort)

	err := http.ListenAndServe(serverPort, nil)
	if err != nil {
		log.Fatal(err)
	}
}

func getPost(w http.ResponseWriter, r *http.Request) {
	startTime := time.Now()
	id := r.URL.Query().Get("id")
	val, err := getFromRedis(id)
	var result map[string]interface{}

	if err != nil {
		fmt.Printf("==== CACHE MISS ON %s ====\n", redisKey+id)
		res, err := getFromMongo(id)

		if errors.Is(err, mongo.ErrNoDocuments) {
			fmt.Printf("==== DB MISS ON %s ====\n", redisKey+id)
			url := "https://jsonplaceholder.typicode.com/posts/" + id
			response, err := http.Get(url)

			if err != nil {
				handleError(w, "Error fetching data", err)
				return
			}

			err = json.NewDecoder(response.Body).Decode(&result)

			if err != nil {
				handleError(w, "Error decoding JSON", err)
				return
			}

			_ = saveToMongo(id, result)
		} else {
			fmt.Printf("==== DB HIT ON %s ====\n", redisKey+id)
			result = res
		}

		_ = saveToRedis(redisKey+id, result)
	} else {
		fmt.Printf("==== CACHE HIT ON %s ====\n", redisKey+id)
		err := json.Unmarshal([]byte(val), &result)

		if err != nil {
			handleError(w, "Error decoding JSON", err)
			return
		}
	}

	elapsedTime := time.Since(startTime).Seconds() * 1000
	fmt.Printf("Execution time: %.5f milliseconds\n", elapsedTime)
	resultStr, _ := mapToString(result)
	w.Write([]byte(resultStr))
}

func saveToMongo(id string, doc bson.M) error {
	client, collection := getMongoClientAndCollection()
	defer client.Disconnect(context.Background())

	idInt, _ := strconv.Atoi(id)
	filter := bson.D{{"id", idInt}}
	existingDoc := collection.FindOne(context.Background(), filter)

	if existingDoc.Err() == nil {
		fmt.Println("Document already exists.")
		return fmt.Errorf("document already exists")
	}

	_, err := collection.InsertOne(context.Background(), doc)
	return err
}

func getFromMongo(id string) (bson.M, error) {
	client, collection := getMongoClientAndCollection()
	defer client.Disconnect(context.Background())

	idInt, _ := strconv.Atoi(id)
	filter := bson.D{{"id", idInt}}
	var result bson.M
	err := collection.FindOne(context.TODO(), filter).Decode(&result)

	return result, err
}

func saveToRedis(key string, val bson.M) error {
	client := getRedisClient()
	defer client.Close()

	value, err := convertBsonMtoJSON(val)

	if err != nil {
		return fmt.Errorf("can't convert bson to json")
	}

	strVal, _ := mapToString(value)
	err = client.Set(key, strVal, expiration).Err()

	return err
}

func getFromRedis(postID string) (string, error) {
	client := getRedisClient()
	defer client.Close()

	val, err := client.Get(redisKey + postID).Result()

	if err == redis.Nil {
		return "", fmt.Errorf("not found")
	} else if err != nil {
		return "", fmt.Errorf("Error getting value: %v", err)
	}

	return val, nil
}

func getMongoClientAndCollection() (*mongo.Client, *mongo.Collection) {
	clientOptions := options.Client().ApplyURI(mongoURI)
	client, err := mongo.Connect(context.TODO(), clientOptions)

	if err != nil {
		log.Fatal(err)
	}

	err = client.Ping(context.TODO(), nil)

	if err != nil {
		log.Fatal("Can't connect to the database")
	}

	collection := client.Database(redisDB).Collection(redisCollection)
	return client, collection
}

func getRedisClient() *redis.Client {
	return redis.NewClient(&redis.Options{
		Addr:     redisAddr,
		Password: redisPassword,
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

func mapToString(inputMap map[string]interface{}) (string, error) {
	jsonData, err := json.Marshal(inputMap)
	return string(jsonData), err
}

func handleError(w http.ResponseWriter, message string, err error) {
	fmt.Printf("%s: %v\n", message, err)
	http.Error(w, message, http.StatusInternalServerError)
}
