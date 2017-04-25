#include <stdio.h>
#include <stdlib.h>
int main(int argc,char** argv)

{

FILE* out = fopen("input.txt","w");
int i;
int n = atoi(argv[1]);
for(i=1;i<=n;i+=10)
{
	fprintf(out,"%d %d %d %d %d %d %d %d\n",i,i+1,i+2,i+5,i+6,i+7,i+8,i+9);
	fprintf(out,"%d %d %d %d %d %d\n",i+1,i,i+2,i+3,i+4,i+7);
	fprintf(out,"%d %d %d %d %d %d\n",i+2,i,i+1,i+3,i+4,i+5);
	fprintf(out,"%d %d %d %d\n",i+3,i+1,i+2,i+4);
	fprintf(out,"%d %d %d %d\n",i+4,i+1,i+2,i+3);
	fprintf(out,"%d %d %d %d\n",i+5,i,i+2,i+8);
	fprintf(out,"%d %d %d %d\n",i+6,i,i+7,i+9);
	fprintf(out,"%d %d %d %d\n",i+7,i,i+1,i+6);
	fprintf(out,"%d %d %d %d\n",i+8,i,i+5,i+9);
	fprintf(out,"%d %d %d %d\n",i+9,i,i+6,i+8);

}
return 0;
}
