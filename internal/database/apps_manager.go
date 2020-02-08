package database

import (
	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"
	"log"
)

func GetApps() []App {

	var apps []App

	query := func(c *mgo.Collection) error {
		return c.Find(nil).All(&apps)
	}

	err := executeQuery(appsCollectionName, query)
	if err != nil {
		log.Fatal(err)
	}
	return apps
}

func GetAppById(appId string) App {

	var user App

	if !bson.IsObjectIdHex(appId) {
		return user
	}

	query := func(c *mgo.Collection) error {
		return c.FindId(appId).One(&user)
	}

	executeQuery(appsCollectionName, query)

	return user
}

func GetAppByKey(key string) App {

	var app App

	query := func(c *mgo.Collection) error {
		return c.Find(bson.M{"key": key}).One(&app)
	}

	executeQuery(appsCollectionName, query)

	return app
}
