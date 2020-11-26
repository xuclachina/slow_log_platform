package main

import (
	"bufio"
	"bytes"
	"crypto/md5"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"time"
)
const middle = "========="

type Config struct {
	Mymap  map[string]string
	strcet string
}

func (c *Config) InitConfig(path string) {
	c.Mymap = make(map[string]string)

	f, err := os.Open(path)
	if err != nil {
		panic(err)
	}
	defer f.Close()

	r := bufio.NewReader(f)
	for {
		b, _, err := r.ReadLine()
		if err != nil {
			if err == io.EOF {
				break
			}
			panic(err)
		}

		s := strings.TrimSpace(string(b))
		//fmt.Println(s)
		if strings.Index(s, "#") == 0 {
			continue
		}

		n1 := strings.Index(s, "[")
		n2 := strings.LastIndex(s, "]")
		if n1 > -1 && n2 > -1 && n2 > n1+1 {
			c.strcet = strings.TrimSpace(s[n1+1 : n2])
			continue
		}

		if len(c.strcet) == 0 {
			continue
		}
		index := strings.Index(s, "=")
		if index < 0 {
			continue
		}

		frist := strings.TrimSpace(s[:index])
		if len(frist) == 0 {
			continue
		}
		second := strings.TrimSpace(s[index+1:])

		pos := strings.Index(second, "\t#")
		if pos > -1 {
			second = second[0:pos]
		}

		pos = strings.Index(second, " #")
		if pos > -1 {
			second = second[0:pos]
		}

		pos = strings.Index(second, "\t//")
		if pos > -1 {
			second = second[0:pos]
		}

		pos = strings.Index(second, " //")
		if pos > -1 {
			second = second[0:pos]
		}

		if len(second) == 0 {
			continue
		}

		key := c.strcet + middle + frist
		c.Mymap[key] = strings.TrimSpace(second)
	}
}

func (c Config) Read(node, key string) string {
	key = node + middle + key
	v, found := c.Mymap[key]
	if !found {
		return ""
	}
	return v
}

func readConfig(configPath string) (slowLog, maxSize, dbid, url, metadir string) {
	myConfig := new(Config)
	myConfig.InitConfig(configPath)
	//fmt.Println(myConfig.Read("default", "path"))
	slowLog = myConfig.Read("slowlog", "filename")
	maxSize = myConfig.Read("slowlog", "max_size ")
	dbid = myConfig.Read("instance", "dbid")
	url = myConfig.Read("server", "url")
	metadir = myConfig.Read("meta", "dir")
	return slowLog, maxSize, dbid, url, metadir
}

func readSlowLogToList(fileName string, lastPos int64, read_size int64) (sqltxt [][]string) {
	//# 组合每一分列表[],[]...
	sqltxt = [][]string{}
	//# 每组分列表
	sql := []string{}
	//# 拼接多个SQL语句
	output := ""
	//# 设置分组列表标识
	isflag := 1
	f, err := os.Open(fileName)
	if err != nil {
		log.Panic("read fail   ", fileName)
	}
	_, _ = f.Seek(lastPos, 0)
	defer f.Close()
	rd := bufio.NewReader(f)
	for {
		line, err := rd.ReadString('\n') //以'\n'为结束符读入一行
		if err != nil || io.EOF == err {
			break
		}
		//fmt.Println(line)
		line = strings.TrimSpace(line)
		if strings.HasPrefix(line, "#") || strings.HasPrefix(line, "SET") {
			sql = append(sql, line)
		} else if strings.HasPrefix(line, "USE") || strings.HasPrefix(line, "use") {
			continue
		} else {
			if strings.HasSuffix(line, ";") {
				if len(output) == 0 {
					sql = append(sql, line)
					isflag = 0
				} else {
					line = output + " " + line
					sql = append(sql, line)
					output = ""
					isflag = 0
				}
			} else {
				output += " " + line
			}
		}
		if isflag == 0 {
			sqltxt = append(sqltxt, sql)
			isflag = 1
			sql = []string{}
		}
	}
	return sqltxt
}

func handlerSlowlog(fileName string, lastPos int64, readSize int64, dbid int64, url string) {
	result := readSlowLogToList(fileName, lastPos, readSize)
	//fmt.Println(result)
	for res := range result {
		//fmt.Println(result[res])
		slowDict := map[string]interface{}{}
		slowDict["dbid"] = int(dbid)
		//	 # user部分处理
		userhost := result[res][1]
		userhost = strings.TrimSpace(userhost)
		dbUser := strings.TrimSpace(strings.Split(strings.ReplaceAll(userhost, "# User@Host:", ""), "[")[0])
		slowDict["db_user"] = dbUser
		appIp := strings.ReplaceAll(userhost, "# User@Host:", "")
		appIp = strings.Split(appIp, " ")[4]
		appIp = strings.ReplaceAll(appIp, "[", "")
		appIp = strings.ReplaceAll(appIp, "]", "")
		slowDict["app_ip"] = appIp
		threadId := strings.TrimSpace(strings.Split(strings.ReplaceAll(userhost, "# User@Host:", ""), ":")[1])
		slowDict["thread_id"], _ = strconv.Atoi(threadId)
		//# querytime部分处理
		querytime := result[res][2]
		querytime = strings.TrimSpace(querytime)
		execDuration := strings.Split(strings.ReplaceAll(querytime, "# ", ""), " ")[1]
		slowDict["exec_duration"] = execDuration
		rowsSent := strings.ReplaceAll(querytime, "# ", "")
		rowsSent = strings.Split(rowsSent, " ")[6]
		slowDict["rows_sent"], _ = strconv.Atoi(rowsSent)
		rowsExamined := strings.ReplaceAll(querytime, "# ", "")
		rowsExamined = strings.Split(rowsExamined, " ")[9]
		slowDict["rows_examined"], _ = strconv.Atoi(rowsExamined)
		//# starttime部分处理
		startTime := strings.Split(strings.ReplaceAll(result[res][3], ";", ""), "=")[1]
		slowDict["start_time"], _ = strconv.Atoi(startTime)
		//# sql部分处理
		line := result[res][4]
		slowDict["orig_sql"] = line
		re3, _ := regexp.Compile("\\d+")
		lineD := re3.ReplaceAllString(line, "?")
		re4, _ := regexp.Compile("([\\'\\\"]).+?([\\'\\\"])")
		lineS := re4.ReplaceAllString(lineD, "?")
		re5, _ := regexp.Compile("\\(\\?.+?\\)")
		sqlParttern := re5.ReplaceAllString(lineS, "(?)")
		slowDict["sql_pattern"] = sqlParttern
		if strings.HasPrefix(sqlParttern, "throttle") || strings.HasPrefix(sqlParttern, "#") {
			continue
		}
		//# fingerprint处理
		slowDict["fingerprint"] = md5V(sqlParttern)
		//fmt.Println(slowDict)
		ret := Post(url, slowDict, "application/json")
		log.Println(ret)
	}
	return
}
func md5V(str string) string {
	h := md5.New()
	h.Write([]byte(str))
	return hex.EncodeToString(h.Sum(nil))
}
func Post(url string, data interface{}, contentType string) string {

	// 超时时间：5秒
	client := &http.Client{Timeout: 5 * time.Second}
	jsonStr, _ := json.Marshal(data)
	resp, err := client.Post(url, contentType, bytes.NewBuffer(jsonStr))
	if err != nil {
		log.Println(err, "发送失败")
	}
	defer resp.Body.Close()
	result, _ := ioutil.ReadAll(resp.Body)
	log.Println("发送成功")
	return string(result)
}

func updatePos(pos string, metafile string) {
	_ = ioutil.WriteFile(metafile, []byte(pos), 0777)
}
func getFileSize(slowLog string) (fileSize int64) {
	var result int64
	_ = filepath.Walk(slowLog, func(path string, f os.FileInfo, err error) error {
		result = f.Size()
		return nil
	})
	return result
}

func Read0(filename string) string {
	f, err := ioutil.ReadFile(filename)
	if err != nil {
		fmt.Println("read fail", err)
	}
	return string(f)
}
func getLastPos(metadir string) int64 {
	metaFile := filepath.Join(metadir, "meta/lastposition")
	_, err := os.Stat(metaFile)
	if err != nil {
		metaDir := filepath.Join(metadir, "meta/")
		_ = os.MkdirAll(metaDir, os.ModePerm)
		return 0
	}
	lastPos := Read0(metaFile)
	lastPos_, _ := strconv.Atoi(lastPos)
	return int64(lastPos_)
}

func main() {
	for {
		slowLog, maxSize, dbid, url, metadir := readConfig("conf/slow_config.ini")
		_ = maxSize
		metafile := filepath.Join(metadir, "meta/lastposition")
		lastPos := getLastPos(metadir)
		curPos := getFileSize(slowLog)
		if lastPos == curPos {
			log.Println("本次无需处理")
			time.Sleep(time.Duration(10) * time.Second)
			continue
		}
		log.Printf("上次位点：%v 本次位点：%v", lastPos, curPos)
		dbidInt, _ := strconv.Atoi(dbid)
		handlerSlowlog(slowLog, lastPos, curPos-lastPos, int64(dbidInt), url)
		curPosStr := strconv.FormatInt(curPos, 10)
		updatePos(curPosStr, metafile)
		//break
	}
}
