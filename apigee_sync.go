package apidApigeeSync

import (
	"github.com/30x/apid-core"
	"net/http"
	"time"
)

const (
	httpTimeout   = time.Minute
	pluginTimeout = time.Minute
)

var knownTables = make(map[string]bool)

/*
 *  Start from existing snapshot if possible
 *  If an existing snapshot does not exist, use the apid scope to fetch
 *  all data scopes, then get a snapshot for those data scopes
 *
 *  Then, poll for changes
 */
func bootstrap() {

	if apidInfo.LastSnapshot != "" {
		snapshot := startOnLocalSnapshot(apidInfo.LastSnapshot)

		events.EmitWithCallback(ApigeeSyncEventSelector, snapshot, func(event apid.Event) {
			changeManager.pollChangeWithBackoff()
		})

		log.Infof("Started on local snapshot: %s", snapshot.SnapshotInfo)
		return
	}

	downloadBootSnapshot(nil)
	downloadDataSnapshot(quitPollingSnapshotServer)

	changeManager.pollChangeWithBackoff()

}

/*
 * Call toExecute repeatedly until it does not return an error, with an exponential backoff policy
 * for retrying on errors
 */
func pollWithBackoff(quit chan bool, toExecute func(chan bool) error, handleError func(error)) {

	backoff := NewExponentialBackoff(200*time.Millisecond, config.GetDuration(configPollInterval), 2, true)

	//inintialize the retry channel to start first attempt immediately
	retry := time.After(0 * time.Millisecond)

	for {
		select {
		case <-quit:
			log.Info("Quit signal recieved.  Returning")
			return
		case <-retry:
			start := time.Now()

			err := toExecute(quit)
			if err == nil {
				return
			}

			if _, ok := err.(quitSignalError); ok {
				return
			}

			end := time.Now()
			//error encountered, since we would have returned above otherwise
			handleError(err)

			/* TODO keep this around? Imagine an immediately erroring service,
			 *  causing many sequential requests which could pollute logs
			 */
			//only backoff if the request took less than one second
			if end.After(start.Add(time.Second)) {
				backoff.Reset()
				retry = time.After(0 * time.Millisecond)
			} else {
				retry = time.After(backoff.Duration())
			}
		}
	}
}

func Redirect(req *http.Request, _ []*http.Request) error {
	req.Header.Add("Authorization", "Bearer "+tokenManager.getBearerToken())
	req.Header.Add("org", apidInfo.ClusterID) // todo: this is strange.. is it needed?
	return nil
}

func addHeaders(req *http.Request) {
	req.Header.Add("Authorization", "Bearer "+ tokenManager.getBearerToken())
	req.Header.Set("apid_instance_id", apidInfo.InstanceID)
	req.Header.Set("apid_cluster_Id", apidInfo.ClusterID)
	req.Header.Set("updated_at_apid", time.Now().Format(time.RFC3339))
}

type changeServerError struct {
	Code string `json:"code"`
}

type quitSignalError struct {
}

type expected200Error struct {
}

func (an expected200Error) Error() string {
	return "Did not recieve OK response"
}

func (a quitSignalError) Error() string {
	return "Signal to quit encountered"
}

func (a changeServerError) Error() string {
	return a.Code
}