#include "s3select.h"
#include <fstream>
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>

using namespace s3selectEngine;
using namespace BOOST_SPIRIT_CLASSIC_NS;

int cli_get_schema(const char* input_schema, actionQ& x)
{
  g_push_column.set_action_q(&x);

  rule<> column_name_rule = lexeme_d[(+alpha_p >> *digit_p)];

  //TODO an issue to resolve with trailing space
  parse_info<> info = parse(input_schema, ((column_name_rule)[BOOST_BIND_ACTION(push_column)] >> *(',' >> (column_name_rule)[BOOST_BIND_ACTION(push_column)])), space_p);

  if (!info.full)
  {
    std::cout << "failure in schema description " << input_schema << std::endl;
    return -1;
  }

  return 0;
}

int main(int argc, char** argv)
{

  //purpose: demostrate the s3select functionalities
  s3select s3select_syntax;

  char*  input_query = 0;

  for (int i = 0; i < argc; i++)
  {

    if (!strcmp(argv[i], "-q"))
    {
      input_query = argv[i + 1];
    }
  }


  if (!input_query)
  {
    std::cout << "type -q 'select ... from ...  '" << std::endl;
    return -1;
  }


  bool to_aggregate = false;

  int status = s3select_syntax.parse_query(input_query);
  if (status != 0)
  {
    std::cout << "failed to parse query " << s3select_syntax.get_error_description() << std::endl;
    return -1;
  }

  std::string object_name = s3select_syntax.get_from_clause(); //TODO stdin

  FILE* fp;

  if (object_name.compare("stdin")==0)
  {
    fp = stdin;
  }
  else
  {
    fp  = fopen(object_name.c_str(), "r");
  }


  if(!fp)
  {
    std::cout << " input stream is not valid, abort;" << std::endl;
    return -1;
  }

  struct stat statbuf;

  lstat(object_name.c_str(), &statbuf);

  std::string s3select_result;
  s3selectEngine::csv_object::csv_defintions csv;
  csv.use_header_info = false;
  //csv.column_delimiter='|';
  //csv.row_delimiter='\t';


  s3selectEngine::csv_object  s3_csv_object(&s3select_syntax, csv);
  //s3selectEngine::csv_object  s3_csv_object(&s3select_syntax);

#define BUFF_SIZE 1024*1024*4
  char* buff = (char*)malloc( BUFF_SIZE );
  while(1)
  {
    //char buff[4096];

    //char * in = fgets(buff,sizeof(buff),fp);
    size_t input_sz = fread(buff, 1, BUFF_SIZE, fp);
    char* in=buff;
    //input_sz = strlen(buff);
    //size_t input_sz = in == 0 ? 0 : strlen(in);

    //if (!input_sz) to_aggregate = true;


    //int status = s3_csv_object.run_s3select_on_object(s3select_result,in,input_sz,false,false,to_aggregate);
    int status = s3_csv_object.run_s3select_on_stream(s3select_result, in, input_sz, statbuf.st_size);
    if(status<0)
    {
      std::cout << "failure on execution " << std::endl;
      break;
    }

    if(s3select_result.size()>1)
    {
      std::cout << s3select_result;
    }

    s3select_result = "";
    if(!input_sz || feof(fp))
    {
      break;
    }

  }

  free(buff);
  fclose(fp);


}
