import HTMLParser
import urllib2
import re
import os
import threading
import download_worker

#html parsers.
class CaidaParser(HTMLParser.HTMLParser):
	def __init__(self):
		HTMLParser.HTMLParser.__init__(self);
		self.img_cnt=0;
		self.alt="";
		self.file=[];
		self.dir=[];

	def get_attr_value(self, target, attrs):
		for e in attrs:
			key = e[0];
			value = e[1];
			if (key == target):
				return value;

	def handle_starttag(self, tag, attrs):
		if (tag == "img"):
			if (self.img_cnt >=2):
				alt_value = self.get_attr_value("alt", attrs);
				self.alt=alt_value;
			self.img_cnt = self.img_cnt + 1;
		
		if (tag == "a" and self.alt == "[DIR]"):
			href_value = self.get_attr_value("href", attrs);
			self.dir.append(href_value);
		elif (tag == "a" and self.alt != ""):
			href_value = self.get_attr_value("href", attrs);
			self.file.append(href_value);

def read_auth(auth_file, account):
	ret = [];

	is_provided = False;
	for line in open(auth_file, 'r'):
		if (line=="\n"):
			continue;
		if (is_provided and len(re.findall("#",line)) ==0):
			ret.append(line.strip('\n'));
		elif(is_provided):
			break;

		if (len(re.findall("#"+account,line)) != 0):
			is_provided = True;
	return ret;

def get_time_list(list_file_name, time):
	str = time;
	target = "";
	res = [];
	
	is_included = False;
	for line in open(list_file_name, 'r'):
		t = line.split('/')[1];
		if (t == time):
			is_included = True;
			target = line;
			url = target.split(':', 1)[1];
			url = url.strip('\n');
			res.append(url);
	
	if (not is_included):
		return None;

	return res;

#must be of the same length.
def time_cmp(t1, t2):
	for i in range(len(t1)):
		if (t1[i] != t2[i]):
			break;
	if (i < len(t1)):
		return int(t1[i]) - int(t2[i]);
	return 0;

def get_latest_time(file_name):
	f = open(file_name, 'rb');
	old_time = "00000000";
	for line in f.readlines():
		time = line.split('/')[-2].split('-')[1];
		if (time_cmp(time ,old_time) > 0):
			old_time = time;
	f.close();
	
	return old_time;

def update_caida_tree(old_time, dir, username, password):
	url = "https://topo-data.caida.org/team-probing/list-7.allpref24/";
	passwd_mgr = urllib2.HTTPPasswordMgrWithDefaultRealm();
	passwd_mgr.add_password("topo-data", url, username, password);

	if (not os.path.exists("caida")):
		f = open("caida",'wb');
		f.close();
	file = open("caida",'a');

	opener = urllib2.build_opener(urllib2.HTTPBasicAuthHandler(passwd_mgr));
	team_dir = ["team-1/daily/", "team-2/daily/", "team-3/daily/"]; 

	for t in team_dir:
		f = opener.open(url+t);
		text = f.read();
		parser = CaidaParser();
		parser.feed(text);
		
		team = t.split('/')[0];
		old_year = old_time[:4];
	
		for e in parser.dir:
			if(time_cmp(e.strip('/'), old_year) >= 0):
				update_year_dir(old_time, url+t+e, dir+team+"/", opener, file);
	
	file.close();

	return "";

def update_year_dir(old_time, url, dir, opener, file):
	f = opener.open(url);
	text = f.read();
	
	parser = CaidaParser();
	parser.feed(text);

	for e in parser.dir:
		time = e.split('-')[1].strip('/');
		if (time_cmp(time, old_time) > 0):
			update_time_dir(url+e, dir+time+"/", opener, file);

	return "";

def update_time_dir(url, dir, opener, file):
	f = opener.open(url);
	text = f.read();
	
	parser = CaidaParser();
	parser.feed(text);

	for e in parser.file:
		if ( len(e.split('.')) != 8 ):
			continue;
		node = e.split('.')[5];
		node_dir = dir+node+"/";
		file.write(node_dir+":"+url+e+'\n');
		print node_dir;

def download_caida_restricted_worker_mt_wrapper(url, dir, file, username, password, res_list, ind, proxy=""):
	res = download_worker.download_caida_restricted_worker(url, dir, file, username, password, proxy);
	res_list[ind] = res;

def download_time(time, list_file_name="caida", root_dir="data/caida/ipv4/", proxy_file="", mt_num=0 ):
	auth = read_auth("auth", "caida");
	url_list = get_time_list(list_file_name, time);

	dir = root_dir+time+"/";
	if (not os.path.exists(dir)):
		os.makedirs(dir);
	
	if (mt_num == 0):
		for url in url_list:
			team = url.split('/')[5];
			suffix = url.split('/')[-1].split('.',4)[-1];
			file = team+"."+suffix;
			if( not os.path.exists(dir+file) ):
				res = False;
				while(not res):
					res = download_worker.download_caida_restricted_worker(url, dir, file, auth[0], auth[1]) 

	elif (mt_num >= 1):
		is_finished = [False for i in range(len(url_list))];
		proxy_list = [];
		fp = open(proxy_file,'rb');
		for line in fp.readlines():
			proxy_list.append(line.strip('\n'));
		cur_proxy = 0;
		
		while(True):
			task_list = [];
			th_pool = [];
			for i in range(len(url_list)):
				if (not is_finished[i]):
					task_list.append(i);
					
			if (len(task_list) == 0):
				break;
			
			cnt = 0;
			for i in range(len(task_list)):
				url = url_list[task_list[i]];
				team = url.split('/')[5];
				suffix = url.split('/')[-1].split('.',4)[-1];
				file = team+"."+suffix;
				ind = task_list[i];
				proxy = proxy_list[cur_proxy];
				cur_proxy = cur_proxy + 1;
				if (cur_proxy >= len(proxy_list)):
					cur_proxy = 0;

				th = threading.Thread(target=download_caida_restricted_worker_mt_wrapper, args=( url,dir,file,auth[0],auth[1],is_finished,ind,proxy,) );
				th_pool.append(th);
				cnt = cnt + 1;
				
				if (cnt >= mt_num or i==len(task_list)-1):
					for th in th_pool:
						th.start();
					for th in th_pool:
						th.join();
					cnt = 0;
					th_pool = [];

#update_caida_tree("00000000", "", "15b903031@hit.edu.cn", "yuzhuoxun123");
#update_caida_tree(get_latest_time("caida"), "", "15b903031@hit.edu.cn", "yuzhuoxun123");
#download_time("20160628");
download_time("20160628", proxy_file="proxy_list", mt_num=30);
