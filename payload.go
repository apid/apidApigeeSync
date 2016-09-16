package apidApigeeSync

type Payload struct {
	Email          string   `json:"email"`
	FirstName      string   `json:"firstName"`
	LastName       string   `json:"lastName"`
	UserName       string   `json:"userName"`
	Organization   string   `json:"organizationName"`
	Status         string   `json:"status"`
	CreatedAt      int64    `json:"createdAt"`
	CreatedBy      string   `json:"createdBy"`
	LastModifiedAt int64    `json:"lastModifiedAt"`
	LastModifiedBy string   `json:"lastModifiedBy"`
	AppId          string   `json:"appId"`
	AppFamily      string   `json:"appFamily"`
	ConsumerSecret string   `json:"consumerSecret"`
	IssuedAt       int64    `json:"issuedAt"`
	DeveloperId    string   `json:"developerId"`
	CallbackUrl    string   `json:"callbackUrl"`
	AppName        string   `json:"name"`
	ApiProducts    []Apip   `json:"apiProducts"`
	Environments   []string `json:"environments"`
	Resources      []string `json:"apiResources"`
	URL            string   `json:"url"`
	Type           int      `json:"type"`
	ParentId       string   `json:"parentId"`
	Etag           string   `json:"etag"`
	Customtag      string   `json:"customtag"`
	Manifest       string   `json:"manifest"`
}

type DataPayload struct {
	EntityIdentifier string  `json:"entityIdentifier"`
	EntityType       string  `json:"entityType"`
	Operation        string  `json:"operation"`
	PldCont          Payload `json:"entityPayload"`
}

type ChangePayload struct {
	LastMsId int64       `json:"_id"`
	Ts       int64       `json:"_ts"`
	Tags     []string    `json:"tags"`
	Data     DataPayload `json:"data"`
}

type ChangeSet struct {
	AtStart bool            `json:"atStart"`
	AtEnd   bool            `json:"atEnd"`
	Changes []ChangePayload `json:"changes"`
}

type Apip struct {
	ApiProduct string `json:"apiproduct"`
	Status     string `json:"status"`
}
