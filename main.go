package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"regexp"
	"sort"
	"time"

	"github.com/sirupsen/logrus"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"gopkg.in/mgo.v2/bson"
)

var (
	log = logrus.WithField("pkg", "watchstream")
)

func connect(outcli **mongo.Client, url string) error {
	since := time.Now()
	opt := options.Client().ApplyURI(url)
	maxConnIdleTime := time.Hour
	opt.MaxConnIdleTime = &maxConnIdleTime
	maxPollSize := uint64(200)
	opt.MaxPoolSize = &maxPollSize
	opt.SetSocketTimeout(time.Minute * 10)
	client, err := mongo.NewClient(opt)
	if err != nil {
		return fmt.Errorf("mongo.NewClient:%v", err)
	}

	err = client.Connect(context.Background())
	if err != nil {
		return err
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := client.Ping(ctx, nil); err != nil {
		return err
	}

	log.WithFields(logrus.Fields{
		"takeTime": time.Now().Sub(since),
	}).Info("Mongo connected")

	*outcli = client

	return nil
}

type ChangeDocNs struct {
	Database   string "db"
	Collection string "coll"
}

type ChangeDoc struct {
	DocKey            map[string]interface{} "documentKey"
	Id                interface{}            "_id"
	Operation         string                 "operationType"
	FullDoc           map[string]interface{} "fullDocument"
	Namespace         ChangeDocNs            "ns"
	Timestamp         primitive.Timestamp    "clusterTime"
	UpdateDescription map[string]interface{} "updateDescription"
}

var mongoUrl = flag.String("url", "", "mongo url")

func main() {
	flag.Parse()

	var client *mongo.Client
	if err := connect(&client, *mongoUrl); err != nil {
		log.WithError(err).Fatalln("ConnectMongoFailed")
		return
	}

	dbs, err := client.ListDatabaseNames(nil, bson.M{})
	if err != nil {
		log.WithError(err).Fatalln("ListDatabaseNames")
		return
	}
	targetDBs := []string{}

	targetDBRegex := regexp.MustCompile("^cryptobroker_prod_.*_00[0-9]+$")
	for _, dbName := range dbs {
		if !targetDBRegex.MatchString(dbName) {
			continue
		}
		targetDBs = append(targetDBs, dbName)
	}
	targetColls := []string{"orders", "trades"}

	sort.Strings(targetDBs)

	for _, dbName := range targetDBs {
		for _, collName := range targetColls {
			coll := client.Database(dbName).Collection(collName)
			log.WithField("ns", dbName+"."+collName).Infoln("StartWatch")

			go func() {
				stream, err := coll.Watch(
					context.Background(),
					bson.M{},
					options.ChangeStream().
						SetFullDocument(options.UpdateLookup),
				)

				if err != nil {
					log.WithError(err).Fatalln("CollectionWatch")
					return
				}
				defer stream.Close(context.Background())

				for {
					if !stream.Next(context.Background()) {
						time.Sleep(time.Second * 10)
					}

					var doc ChangeDoc
					if err := stream.Decode(&doc); err != nil {
						log.WithError(err).Fatalln("StreamDecode")
						return
					}

					ret, _ := json.Marshal(doc)
					logrus.Infoln("ChangeStreamEvent", string(ret))
				}
			}()
		}
	}
}
