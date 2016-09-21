package main

import (
	"bytes"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net"
	"os"
	"os/exec"
	"strings"
	"sync"
	"time"

	"golang.org/x/crypto/ssh"
	"golang.org/x/crypto/ssh/agent"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/ec2"
	"github.com/aws/aws-sdk-go/service/elb"
)

var (
	flagD = flag.Bool("d", false, "delete instances from previous runs")

	appName      = "benchcore"
	testRunID    = appName + randString()
	ami          = "ami-f71883e0" // Ubuntu LTS 16.04
	instanceType = "m3.large"
	subnetID     = "subnet-80560fd9"
	key          = os.Getenv("USER")
	user         = os.Getenv("USER")
	schemaPath   = os.Getenv("CHAIN") + "/core/schema.sql"
	sdkDir       = os.Getenv("CHAIN") + "/sdk/java"

	awsConfig = &aws.Config{Region: aws.String("us-east-1")}
	ec2client = ec2.New(awsConfig)
	elbclient = elb.New(awsConfig)

	keyring   = sshAgent(os.Getenv("SSH_AUTH_SOCK"))
	sshConfig = &ssh.ClientConfig{
		User: "ubuntu",
		Auth: []ssh.AuthMethod{ssh.PublicKeysCallback(keyring.Signers)},
	}

	killInstanceIDs []*string // instances to terminate on exit
	deleteELBNames  []*string // elbs to delete on exit
)

func sshAgent(socket string) agent.Agent {
	conn, err := net.Dial("unix", socket)
	if err != nil {
		log.Fatal(err)
	}
	return agent.NewClient(conn)
}

type instance struct {
	id       string
	addr     string
	privAddr string
}

func main() {
	log.SetPrefix(appName + ": ")
	log.SetFlags(log.Ldate | log.Lmicroseconds)

	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "Usage: %s [-d] X.java\n", os.Args[0])
		fmt.Fprint(os.Stderr, usage)
		flag.PrintDefaults()
	}
	flag.Parse()

	if *flagD {
		doDelete()
		return
	}

	if flag.NArg() == 0 {
		flag.Usage()
		os.Exit(2)
	}

	progName := flag.Arg(0)
	testJava, err := ioutil.ReadFile(progName)
	must(err)

	schema, err := ioutil.ReadFile(schemaPath)
	must(err)

	c := exec.Command("git", "rev-parse", "HEAD")
	c.Stderr = os.Stderr
	commit, err := c.Output()
	must(err)
	commit = bytes.TrimSpace(commit)

	var (
		db     instance
		client instance
		cored  instance
	)

	var wg sync.WaitGroup
	wg.Add(3)
	log.Println("starting EC2 instances")
	go makeEC2("pg", &db, &wg)
	go makeEC2("cored", &cored, &wg)
	go makeEC2("client", &client, &wg)
	killInstanceIDs = append(killInstanceIDs, &db.id, &cored.id, &client.id)

	coredBin := mustBuildCored()
	corectlBin := mustBuildCorectl()
	chainJAR := mustBuildJAR()

	log.Println("waiting for EC2 instances to open port 22")
	wg.Wait()

	log.Println("init database")
	must(scpPut(db.addr, schema, "schema.sql", 0644))
	must(scpPut(db.addr, corectlBin, "corectl", 0755))
	mustRunOn(db.addr, initdbsh)

	dbURL := "postgres://benchcore:benchcorepass@" + db.privAddr + "/core?sslmode=disable"

	log.Println("init cored hosts")
	must(scpPut(cored.addr, coredBin, "cored", 0755))
	go mustRunOn(cored.addr, coredsh, "dbURL", dbURL)

	log.Println("init client")
	coreURL := "http://" + cored.privAddr + ":8080"
	log.Println("core URL:", coreURL)
	must(scpPut(client.addr, chainJAR, "chain.jar", 0644))
	javaClass := strings.TrimSuffix(progName, ".java")
	must(scpPut(client.addr, testJava, javaClass+".java", 0644))
	mustRunOn(client.addr, clientsh,
		"coreURL", coreURL,
		"coreAddr", cored.privAddr,
		"javaClass", javaClass,
	)
	statsBytes, err := scpGet(client.addr, "stats.json")
	must(err)
	log.Println("SUCCESS")

	stats := make(map[string]interface{})
	must(json.Unmarshal(statsBytes, &stats))
	stats["commit"] = string(commit)
	stats["prog"] = progName
	stats["finished"] = time.Now().UTC()

	out, err := json.MarshalIndent(stats, "", "	")
	must(err)
	os.Stdout.Write(append(out, '\n'))
	cleanup()
}

func doDelete() {
	desc, err := ec2client.DescribeInstances(&ec2.DescribeInstancesInput{
		Filters: []*ec2.Filter{
			{Name: aws.String("tag:Application"), Values: []*string{aws.String("benchcore")}},
			{Name: aws.String("tag:User"), Values: []*string{&user}},
		},
	})
	if err != nil {
		log.Fatal(err)
	}
	for _, res := range desc.Reservations {
		for _, inst := range res.Instances {
			killInstanceIDs = append(killInstanceIDs, inst.InstanceID)
		}
	}

	lbs, err := elbclient.DescribeLoadBalancers(&elb.DescribeLoadBalancersInput{})
	if err != nil {
		log.Fatal(err)
	}
	for _, desc := range lbs.LoadBalancerDescriptions {
		if strings.HasPrefix(*desc.LoadBalancerName, appName+"-"+user+"-") {
			deleteELBNames = append(deleteELBNames, desc.LoadBalancerName)
		}
	}

	cleanup()
}

func mustBuildCored() []byte {
	log.Println("building cored")

	env := []string{
		"GOOS=linux",
		"GOARCH=amd64",
		"CGO_ENABLED=0",
	}

	date := time.Now().UTC().Format(time.RFC3339)
	cmd := exec.Command("go", "build",
		"-tags", "insecure_disable_https_redirect",
		"-ldflags", "-X main.buildDate="+date,
		"-o", "/dev/stdout",
		"chain/cmd/cored",
	)
	cmd.Env = mergeEnvLists(env, os.Environ())
	cmd.Stderr = os.Stderr
	out, err := cmd.Output()
	must(err)
	log.Printf("cored executable: %d bytes", len(out))
	return out
}

func mustBuildCorectl() []byte {
	log.Println("building corectl")

	env := []string{
		"GOOS=linux",
		"GOARCH=amd64",
		"CGO_ENABLED=0",
	}

	cmd := exec.Command("go", "build", "-o", "/dev/stdout", "chain/cmd/corectl")
	cmd.Env = mergeEnvLists(env, os.Environ())
	cmd.Stderr = os.Stderr
	out, err := cmd.Output()
	must(err)
	log.Printf("corectl executable: %d bytes", len(out))
	return out
}

func mustBuildJAR() []byte {
	cmd := exec.Command("mvn", "-Djar.finalName=chain", "package")
	cmd.Dir = sdkDir
	cmd.Stdout = os.Stderr
	cmd.Stderr = os.Stderr
	must(cmd.Run())

	b, err := ioutil.ReadFile(sdkDir + "/target/chain.jar")
	must(err)

	log.Printf("java SDK jar: %d bytes", len(b))
	return b
}

func cleanup() {
	if len(killInstanceIDs) > 0 {
		_, err := ec2client.TerminateInstances(&ec2.TerminateInstancesInput{InstanceIDs: killInstanceIDs})
		if err != nil {
			log.Println(err)
		}
	}

	for _, name := range deleteELBNames {
		_, err := elbclient.DeleteLoadBalancer(&elb.DeleteLoadBalancerInput{
			LoadBalancerName: name,
		})
		if err != nil {
			log.Println(err)
		}
	}
}

func scpPut(host string, data []byte, dest string, mode int) error {
	log.Printf("scp %d bytes to %s", len(data), dest)
	var client *ssh.Client
	retry(func() (err error) {
		client, err = ssh.Dial("tcp", host+":22", sshConfig)
		return
	})
	defer client.Close()
	s, err := client.NewSession()
	if err != nil {
		return err
	}
	s.Stderr = os.Stderr
	s.Stdout = os.Stderr
	w, err := s.StdinPipe()
	if err != nil {
		return err
	}
	go func() {
		defer w.Close()
		fmt.Fprintf(w, "C%04o %d %s\n", mode, len(data), dest)
		w.Write(data)
		w.Write([]byte{0})
	}()

	return s.Run("/usr/bin/scp -tr .")
}

func scpGet(host string, src string) (data []byte, err error) {
	log.Printf("scp from %s", src)
	var client *ssh.Client
	retry(func() (err error) {
		client, err = ssh.Dial("tcp", host+":22", sshConfig)
		return
	})
	defer client.Close()
	s, err := client.NewSession()
	if err != nil {
		return nil, err
	}
	s.Stderr = os.Stderr
	r, err := s.StdoutPipe()
	if err != nil {
		return nil, err
	}

	err = s.Start("/usr/bin/scp </dev/zero -qf " + src)
	if err != nil {
		return nil, err
	}

	var n int
	_, err = fmt.Fscanf(r, "C%04o %d %s\n", new(int), &n, new(string))
	if err != nil {
		return nil, fmt.Errorf("cannot scan scp code: %v", err)
	}
	log.Printf("scp reading %d bytes", n)
	data = make([]byte, n+1)
	read, err := io.ReadFull(r, data)
	if err != nil {
		return nil, fmt.Errorf("read %d of %d bytes: %v", read, n, err)
	}
	if data[len(data)-1] != 0 {
		return nil, errors.New("expected trailing NUL byte")
	}
	data = data[:len(data)-1] // chop off trailing NUL
	err = s.Wait()
	if err != nil {
		return nil, fmt.Errorf("wait: %v", err)
	}
	return data, nil
}

func mustRunOn(host, sh string, keyval ...string) {
	if len(keyval)%2 != 0 {
		log.Fatal("odd params", keyval)
	}
	log.Println("run on", host)
	client, err := ssh.Dial("tcp", host+":22", sshConfig)
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()
	s, err := client.NewSession()
	if err != nil {
		log.Fatal(err)
	}
	s.Stdout = os.Stderr
	s.Stderr = os.Stderr
	for i := 0; i < len(keyval); i += 2 {
		sh = strings.Replace(sh, "{{"+keyval[i]+"}}", keyval[i+1], -1)
	}
	err = s.Run(sh)
	if err != nil {
		log.Fatal(err)
	}
}

func makeEC2(role string, inst *instance, wg *sync.WaitGroup) {
	defer wg.Done()
	runtoken := randString()
	var n int64 = 1

	var resv *ec2.Reservation
	retry(func() (err error) {
		resv, err = ec2client.RunInstances(&ec2.RunInstancesInput{
			ClientToken:  &runtoken,
			ImageID:      &ami,
			InstanceType: &instanceType,
			KeyName:      &key,
			MinCount:     &n,
			MaxCount:     &n,
			SubnetID:     &subnetID,
		})
		return err
	})

	inst.id = *resv.Instances[0].InstanceID

	retry(func() error {
		_, err := ec2client.CreateTags(&ec2.CreateTagsInput{
			Resources: []*string{&inst.id},
			Tags: []*ec2.Tag{
				{Key: aws.String("Application"), Value: &appName},
				{Key: aws.String("User"), Value: &user},
				{Key: aws.String("Run"), Value: &testRunID},
				{Key: aws.String("Role"), Value: &role},
			},
		})
		return err
	})

	var desc *ec2.DescribeInstancesOutput
	retry(func() (err error) {
		desc, err = ec2client.DescribeInstances(&ec2.DescribeInstancesInput{
			InstanceIDs: []*string{&inst.id},
		})
		if err != nil {
			return err
		}
		info := desc.Reservations[0].Instances[0]
		state := info.State
		const (
			running = 16 // see ec2.InstanceState
			pending = 0
		)
		if *state.Code&0xff == pending {
			return errRetry
		} else if *state.Code&0xff != running {
			reason := ""
			if x := info.StateReason; x != nil {
				reason = *x.Message
			}
			return fmt.Errorf("instance %s state %s (%s)", inst.id, *state.Name, reason)
		}
		inst.privAddr = *info.PrivateIPAddress
		inst.addr = *info.PublicIPAddress
		return nil
	})

	retry(func() error {
		conn, err := net.Dial("tcp", inst.addr+":22")
		if err != nil && strings.Contains(strings.ToLower(err.Error()), "refused") {
			return errRetry
		} else if err != nil {
			return err
		}
		conn.Close()
		return nil
	})

}

var errRetry = errors.New("retry")

// retry f until it returns nil.
// wait 500ms in between attempts.
// log err unless it is errRetry.
// after 5 failures, it will call log.Fatal.
// returning errRetry doesn't count as a failure.
func retry(f func() error) {
	for n := 0; n < 5; {
		err := f()
		if err != nil && err != errRetry {
			log.Println("retrying:", err)
			n++
		}
		if err != nil {
			time.Sleep(500 * time.Millisecond)
			continue
		}
		return
	}
	log.Fatal("too many retries")
}

func randString() string {
	b := make([]byte, 10)
	_, err := rand.Read(b)
	if err != nil {
		log.Fatalln(err)
	}
	return hex.EncodeToString(b)
}

func must(err error) {
	if err != nil {
		panic(err)
	}
}

// mergeEnvLists merges the two environment lists such that
// variables with the same name in "in" replace those in "out".
// This always returns a newly allocated slice.
func mergeEnvLists(in, out []string) []string {
	out = append([]string(nil), out...)
NextVar:
	for _, inkv := range in {
		k := strings.SplitAfterN(inkv, "=", 2)[0]
		for i, outkv := range out {
			if strings.HasPrefix(outkv, k) {
				out[i] = inkv
				continue NextVar
			}
		}
		out = append(out, inkv)
	}
	return out
}

const initdbsh = `#!/bin/bash
set -eo pipefail

sudo bash <<EOFSUDO
set -eo pipefail
apt-get update -qq
apt-get install -y -qq postgresql-9.5 postgresql-client-9.5

cat <<EOF >/etc/postgresql/9.5/main/postgresql.conf
data_directory = '/var/lib/postgresql/9.5/main'
hba_file = '/etc/postgresql/9.5/main/pg_hba.conf'
ident_file = '/etc/postgresql/9.5/main/pg_ident.conf'
external_pid_file = '/var/run/postgresql/9.5-main.pid'
listen_addresses = '*'
port = 5432
max_connections = 100
unix_socket_directories = '/var/run/postgresql'
ssl = true
ssl_cert_file = '/etc/ssl/certs/ssl-cert-snakeoil.pem'
ssl_key_file = '/etc/ssl/private/ssl-cert-snakeoil.key'
shared_buffers = 128MB
dynamic_shared_memory_type = posix
log_timezone = 'UTC'
stats_temp_directory = '/var/run/postgresql/9.5-main.pg_stat_tmp'
datestyle = 'iso, mdy'
timezone = 'UTC'
lc_messages = 'en_US.UTF-8'
lc_monetary = 'en_US.UTF-8'
lc_numeric = 'en_US.UTF-8'
lc_time = 'en_US.UTF-8'
default_text_search_config = 'pg_catalog.english'
EOF

cat <<EOF >/etc/postgresql/9.5/main/pg_hba.conf
# TYPE  DATABASE        USER            ADDRESS                 METHOD
local   all             postgres                                peer
local   all             all                                     peer
host    all             all             0.0.0.0/0               md5
host    all             all             ::0/0                   md5
EOF

/etc/init.d/postgresql restart
EOFSUDO

sudo -u postgres bash <<EOFPOSTGRES
set -eo pipefail
/usr/lib/postgresql/9.5/bin/createdb core
/usr/lib/postgresql/9.5/bin/psql \
	--quiet \
	-c "CREATE USER benchcore WITH PASSWORD 'benchcorepass' SUPERUSER" \
	core
/usr/lib/postgresql/9.5/bin/psql --quiet -f $HOME/schema.sql core
EOFPOSTGRES

export DATABASE_URL='postgres://benchcore:benchcorepass@localhost/core'
$HOME/corectl config-generator
`

const coredsh = `#!/bin/bash
set -eo pipefail
export DATABASE_URL='{{dbURL}}'
./cored
`

const clientsh = `#!/bin/bash
set -eo pipefail


sudo bash <<EOFSUDO
set -eo pipefail

(
	echo 'debconf shared/accepted-oracle-license-v1-1 select true'
	echo 'debconf shared/accepted-oracle-license-v1-1 seen true'
) | debconf-set-selections

mkdir -p /var/cache/oracle-jdk8-installer
cat <<EOF >/var/cache/oracle-jdk8-installer/wgetrc
noclobber = off
dir_prefix = .
dirstruct = off
progress = dot:giga
verbose = off
quiet = on
tries = 5
EOF

add-apt-repository ppa:webupd8team/java
apt-get update -qq
apt-get install -y -qq oracle-java8-installer

EOFSUDO

echo compiling test
export JAVA_HOME=/usr/lib/jvm/java-8-oracle
export CLASSPATH=.:$HOME/chain.jar
javac {{javaClass}}.java
echo compiled test

echo pinging
ping -c 1 {{coreAddr}}
echo pinged

echo curling "{{coreURL}}/debug/vars"
curl -si "{{coreURL}}/debug/vars"
echo curled
export CHAIN_API_URL='{{coreURL}}/'
echo running test driver
java {{javaClass}}
echo all done
`

const usage = `
Command benchcore boots a set of EC2 instances, compiles
cored, corectl, and the Java SDK locally, sets up a postgres
database and chain core on the instances, copies the SDK and
X.java to another instance to serve as the test driver,
and runs the driver.

It expects a full Chain development environment. See
Readme.md in the root of this repo for instructions.

X.java can have any file name. It is expected to have
a public class of the same name containing the entry point.

On successful exit of the test driver, benchcore will delete
the AWS resources it created. If there is a failure, it will
leave the instances running for debugging investigation. To
clean up, run 'benchcore -d'.

Flags:
`