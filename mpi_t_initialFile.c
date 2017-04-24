#include <mpi.h>
#include <iostream>
#include <vector>
#include <stdio.h>
#include <stdlib.h>
#include <sys/time.h>
#include <string>
#include <sstream>
#include <set>

using namespace std;

#define REQUEST 1
#define RESPONSE 2
#define DEBUG

int V;
vector<vector<int> > adjlist;
set<int> requests;
map<int,string> mp;

map<int,string> response;
int size;

int maximum(int a,int b)
{
	return (a>b)?a:b;
}

string ser(int i)
{
	stringstream ss;
	string a="";
	ss << a;
	ss << i;
	ss <<" ";
	cout<<"S: "<<i<<" "<<adjlist[i].size()<<"\n";
	for(int k=0;k<adjlist[i].size();k++)
	{
		ss << adjlist[i][k];
		if(k!=adjlist[i].size()-1)
		ss << " ";
	}
	return ss.str();
}


string serialise(int rank)
{
	int sz = adjlist.size()-1;
	sz = sz/(size-1);
	int i;
	stringstream total;
	total << "";
	int last = sz*rank;
	if(rank == size-1)
		last = adjlist.size()-1;
	for(i=sz*(rank-1)+1;i<=last;i++)
	{
		string s = ser(i);
		total << s;
			
		if(i!=last)
			total << "%";
	}
	
	return total.str();
}

void send_request(int world_rank)
{
	std::map<int,string>::iterator it;
	int i;
	MPI_Request request = MPI_REQUEST_NULL;
	MPI_Status status;
	for(i=1;i<size;i++)
	{
		if((it=mp.find(i)) != mp.end() && i!=world_rank)
		{
			if(request != MPI_REQUEST_NULL)
				MPI_Wait(&request,&status);
			const char* s = (it->second).c_str();
			MPI_Isend((void*)s,(it->second).size(),MPI_CHAR,it->first,REQUEST,MPI_COMM_WORLD,&request);
			#ifdef DEBUG
			printf("[UPDATE]Sending request %s to %d from %d\n",s,it->first,world_rank);	
			#endif
		}
		else if(i!=world_rank)
		{
			if(request != MPI_REQUEST_NULL)
				MPI_Wait(&request,&status);
			MPI_Isend(NULL,0,MPI_CHAR,i,REQUEST,MPI_COMM_WORLD,&request);
		}
	}
}

void recv_request(int rank)
{
		response.clear();
		char* recv_buf = (char*)malloc(sizeof(char)*256);
		MPI_Status status;
		MPI_Request r;
		int i = 0;
		int flag=0;
		while(i< size-2)
		{
	
        		MPI_Iprobe(MPI_ANY_SOURCE, REQUEST, MPI_COMM_WORLD, &flag, &status);
			if(flag)
			{
				#ifdef DEBUG
				printf("[UPDATE] %d Received a arequest\n",rank);
				#endif
				MPI_Recv(recv_buf,100, MPI_CHAR, MPI_ANY_SOURCE, REQUEST, MPI_COMM_WORLD, &status);
			}
			else
			{
				continue;
			}
			
			int count;
			MPI_Get_count(&status,MPI_CHAR,&count);
		
			#ifdef DEBUG
			printf("[COUNT] %d\n",count);
			fflush(stdout);
			#endif
			if(count == 0){
				i++;
				continue;
			}
			recv_buf[count]='\0';
			#ifdef DEBUG
			printf("IMPORTANT : Rank %d Received %s Hey \n",rank,recv_buf);
			#endif

			char* str = recv_buf;
			char* end = str;
			stringstream resp;
			while(*str)
			{

				int n = strtol(str,&end,10);
				string s = ser(n);
				resp << s ;
				end += 1;
				if(*end != '\0')
					resp << "%";
				str = end;
			}
			#ifdef DEBUG
			cout<<"[RESPONSE_DECISION] Response for "<<status.MPI_SOURCE<<" is "<< resp.str()<<"\n";
			#endif
			string g = resp.str();
		
			response[status.MPI_SOURCE] = g;
			i++;
			
	}	
}

void update(int world_rank,int begin,int end)
{
	
	
	int i;
	stringstream req;
	vector<int> upd;
	for(i=begin;i<=end;i++)
		upd.push_back(i);

	for(i=0;i<upd.size();i++)
	{
		int j = 0;
		vector<int> v = adjlist[upd[i]];
		for(j=0;j<v.size();j++)
		{
			if(adjlist[v[j]].size() == 0)
				requests.insert(v[j]);
		}
	}

	set<int>::iterator it;
	int prev = -1;
	for(it = requests.begin(); it!= requests.end();)
	{
		int num = *it;
		int owner =(num-1)/ (V/(size-1))+1;
		
		if(owner > size-1)
			owner = size-1;

		if(owner == prev)
		{
			req << num << " ";
			it++;
			if(it == requests.end())
			{
				string s = req.str();
				#ifdef DEBUG
				cout<<"Rank "<<world_rank<<" Sending requests "<<req.str()<<"to "<<prev<<"\n";
				#endif
				mp[prev] = s;
				req.str("");
				req.clear();
				break;
			}
		}
		else if(prev==-1)
		{
			req << num << " ";
			it++;	
		}
		else
		{
			//cout<<world_rank<< "   "<<num<<" is "<<owner <<" And "<<prev<<"\n";
			string s = req.str();
			#ifdef DEBUG
			cout<<"Rank "<<world_rank<<" Sending "<<requests.size()<< " request "<<req.str()<<"to "<<prev<<"\n";
			#endif
			mp[prev] = s;
			req.str("");
			req.clear();
			req << num << " ";
			it++;
		}
		prev = owner;	
		
	}

	if((req.str()).size()!=0)
	{
		string s = req.str();
		#ifdef DEBUG
		cout<<"Rank "<<world_rank<< " Sending requeddst "<<req.str()<<"to "<<prev<<"\n";
		#endif
		mp[prev] = s;
	}
}



void deserialise_and_update(char* str,int world_rank)
{
	#ifdef DEBUG
	printf("[BUFFER] %s\n",str);
	#endif
	mp.clear();
	int first = 1;
	int curr = 0;
	requests.clear();
	stringstream req;
	vector<int> upd;
	char* end=str;
	while(*str != '\0')
	{
	
		int n = strtol(str,&end,10);
		
		if(first == 1)
		{
			first = 0;
			curr = n;
		}
		else
		{
			adjlist[curr].push_back(n);
			upd.push_back(curr);					
		}
		if(*end == '\0')
			break;
		if(*end == '%')
		{
			first = 1;
		}
		end += 1;
		str = end;
	}
	int i;


	for(i=0;i<upd.size();i++)
	{
		int j = 0;
		vector<int> v = adjlist[upd[i]];
		for(j=0;j<v.size();j++)
		{
			if(adjlist[v[j]].size() == 0)
				requests.insert(v[j]);
		}
	}

	set<int>::iterator it;
	int prev = -1;
	for(it = requests.begin(); it!= requests.end();)
	{
		int num = *it;
		int owner =(num-1)/ (V/(size-1))+1;
		
		if(owner > size-1)
			owner = size-1;

		if(owner == prev)
		{
			req << num << " ";
			it++;
			if(it == requests.end())
			{
				string s = req.str();
				#ifdef DEBUG
				cout<<"Rank "<<world_rank<<" Sending requests "<<req.str()<<"to "<<prev<<"\n";
				#endif
				mp[prev] = s;
				req.str("");
				req.clear();
				break;
			}
		}
		else if(prev==-1)
		{
			req << num << " ";
			it++;	
		}
		else
		{
			cout<<world_rank<< "   "<<num<<" is "<<owner <<" And "<<prev<<"\n";
			string s = req.str();
			#ifdef DEBUG
			cout<<"Rank "<<world_rank<<" Sending "<<requests.size()<< " request "<<req.str()<<"to "<<prev<<"\n";
			#endif
			mp[prev] = s;
			req.str("");
			req.clear();
			req << num << " ";
			it++;
		}
		prev = owner;	
		
	}

	if((req.str()).size()!=0)
	{
		string s = req.str();
		#ifdef DEBUG
		cout<<"Rank "<<world_rank<< " Sending requeddst "<<req.str()<<"to "<<prev<<"\n";
		#endif
		mp[prev] = s;
	}
	
}

void send_response(int world_rank)
{
	int i;
	MPI_Request r = MPI_REQUEST_NULL;
	MPI_Status status;
	std::map<int,string>::iterator it;
	#ifdef DEBUG
	printf("Rank %d sending response \n",world_rank);
	#endif
	for(i=1;i<size;i++)
	{
		if(i == world_rank)
			continue;
		if (r != MPI_REQUEST_NULL)
			MPI_Wait(&r,&status);
		if((it=response.find(i)) == response.end())
		{
			MPI_Isend(NULL,0,MPI_CHAR,i,RESPONSE,MPI_COMM_WORLD,&r);
		#ifdef DEBUG
			printf("[SEND_RESPONSE]\n");
		#endif
		}
		else{
			MPI_Isend((void*)(it->second).c_str(),(it->second).size(),MPI_CHAR,i,RESPONSE,MPI_COMM_WORLD,&r);
		#ifdef DEBUG
			printf("[SEND_RESPONSE] Sending %s to %d\n",(it->second).c_str(),i);
		#endif
		}
	}
				
}

void receive_response(int rank)
{
	char* recv_buf = (char*)malloc(sizeof(char)*100);
	MPI_Status status;
	MPI_Request r;
	#ifdef DEBUG
	printf("Rank %d receiving reposmses \n",rank);
	#endif


	int i = 0;
	int flag = 0;
	while(1)
	{
		MPI_Iprobe(MPI_ANY_SOURCE, RESPONSE, MPI_COMM_WORLD, &flag, &status);
		if(flag)
		{
			MPI_Recv(recv_buf,100, MPI_CHAR, MPI_ANY_SOURCE, RESPONSE, MPI_COMM_WORLD, &status);
		}
		else
		{
			continue;
		}
		
		
		
		int count;
		MPI_Get_count(&status,MPI_CHAR,&count);
		if(count == 0)
		{
			i++;
			if(i == size-2)
				break;
			continue;
		}
		recv_buf[count]='\0';
		#ifdef DEBUG
			printf("[RECEIVE_RESPONSE] Rank %d Received response %s\n",rank,recv_buf);
		#endif
		i++;
		
		deserialise_and_update(recv_buf,rank);
		if(i == size-2)
			break;
		
	}	
	

}


int compact_forward(int rank)
{
	int i,j,k;
	int tri_count = 0;
	int a,b,m = 0;

	int begin = (rank-1)*V/(size-1)+1;
	int end ;
	if(rank == size-1)
		end = V;
	else
		end = (rank)*V/(size-1);
	#ifdef DEBUG
	printf("[BEGIN] %d [END] %d\n",begin,end);
	#endif
	for(i=begin;i<=end;i++)
	{
		vector<int> l = adjlist[i];
		for(j=0;j<l.size();j++)
		{
			if(l[j] > i)
			{
			#ifdef DEBUG
			cout<<"[GREATER] "<<l[j]<<">"<<i<<"\n";
			#endif
			m = 0;
			k = 0;
					a = adjlist[l[j]][k];// u pri
					b = l[m]; // v prime
					while(k+1 != adjlist[l[j]].size() && m+1 != l.size() && a<i && b<i)
					{
						if(a < b)
						{
							a = adjlist[l[j]][k+1];
							k++;
						}
						else if(a > b)
						{
							b = l[m+1];
							m++;
						}
						else
						{
							tri_count++;
							#ifdef DEBUG
							cout<<"[INC] "<<l[j]<<">"<<i<<"\n";
							#endif
							a = adjlist[l[j]][k+1];
							b = l[m+1];
							m++;
							k++;
						}
						
						
					}
			}
		}
	}
	#ifdef DEBUG
	printf("Triangle count : %d\n",tri_count);
	#endif
	return tri_count;
}


int main(int argc, char** argv) {
	
  V = atoi(argv[1]);
  // Initialize the MPI environment
  MPI_Init(NULL, NULL);
  // Find out rank, size
  int world_rank,local_sum;
  MPI_Comm_rank(MPI_COMM_WORLD, &world_rank);
  int world_size;
  MPI_Comm_size(MPI_COMM_WORLD, &world_size);
  size = world_size;
  int tri_count = 0;
  char* line = (char*)malloc(sizeof(char)*256);
  char* end = line;
 	for(int i=0;i<=V;i++)
	{
		vector<int> v;
		adjlist.push_back(v);
	}
 
 if (world_rank == 0) {
  
	/*while(fgets(line,sizeof(char)*256,f))
	{
		first = 1;
		printf("Line: %s\n",line);
		while(*line)
		{
			int n = strtol(line,&end,10);
			if(first == 1)
			{
				first = 0;
				curr = n;
				lno++;
			}
			else
			{
				adjlist[curr].push_back(n);
			}
		
			if(*end == '\0')
				break;
			end += 1;
			line = end;

			printf("%d\n",n);
		}
		
	}*/
	/*
	int i;
	for(i=1;i<world_size;i++)
	{
		string s = serialise(i);
		cout<<"send "<<s<<s.size()<<"\n";
		const char* str = s.c_str();
		MPI_Send((void*)str,s.size(),MPI_CHAR,i,0,MPI_COMM_WORLD);
	}
  	local_sum = 0;
	printf("[TRICOUNT] in rank %d : %d\n",world_rank,local_sum);
	*/
}
else
{
	FILE* f = fopen("input.txt","r");
	int first = 1;
	int curr = -1;
	int lno = 0;
	int begin = ((world_rank-1) * V/(size-1))+1;
	int end = (world_rank*V/(size-1));
	printf("[BEGIN] %d,[END] %d\n",begin,end);
	char* endl = line;
	if(world_rank == size-1)
		end = V;
	while(fgets(line,sizeof(char)*256,f))
	{
		first = 1;
		while(*line)
		{
			int n = strtol(line,&endl,10);
			if(first == 1)
			{
				first = 0;
				curr = n;
				lno++;
			}
			else
			{
				if(lno >= begin && lno <= end)
				{
					printf("Line no %d\n",lno);
					adjlist[curr].push_back(n);
				}
			}

			if(*endl == '\0' )
			{
				printf("Hiffffffffffffffff");
				break;
			}
			endl += 1;
			line = endl;

		}

	}



	if(world_rank == 2)
	{
		printf("a[1] %d\n",adjlist[1].size());
		printf("a[2] %d\n",adjlist[2].size());
		printf("a[3] %d\n",adjlist[3].size());
		printf("a[4] %d\n",adjlist[4].size());
		printf("a[5] %d\n",adjlist[5].size());
	}
	else 
	{

	}
	update(world_rank,begin,end);
/*	
	char* buf = (char*)malloc(sizeof(char)*100);
	MPI_Status s;
	//MPI_Request r;
	MPI_Recv((void*)buf,100,MPI_CHAR,0,0,MPI_COMM_WORLD,&s);
	int sz;
	MPI_Get_count(&s,MPI_CHAR,&sz);
	buf[sz] = '\0';
	//if(world_rank == 1)
	deserialise_and_update(buf,world_rank);
*/
	send_request(world_rank);
	recv_request(world_rank);
	send_response(world_rank);
	#ifdef DEBUG
	printf("world rank %d sent responses \n",world_rank);
	#endif
	receive_response(world_rank);


	local_sum = compact_forward(world_rank);

}	
  MPI_Barrier(MPI_COMM_WORLD);	
  MPI_Reduce(&local_sum,&tri_count,1,MPI_INT,MPI_SUM,0,MPI_COMM_WORLD);

  if(world_rank == 0)
  	printf("[FINALTRICOUNT] Triangle count in graph is %d\n",tri_count);
 
 MPI_Finalize();
}
