#include <iostream>

#include <boost/mpl/vector/vector30.hpp>
// back-end
#include <boost/msm/back/state_machine.hpp>
//front-end
#include <boost/msm/front/state_machine_def.hpp>

#include <vector>

namespace msm = boost::msm;
namespace mpl = boost::mpl;

namespace s3selectEngine
{
// events
struct event_column_sep {};
struct event_eol {};
struct event_end_of_stream {};
struct event_not_column_sep {};//i.e any char
struct event_quote {};
struct event_escape {};
struct event_empty {};


// front-end: define the FSM structure
struct csvStateMch_ : public msm::front::state_machine_def<csvStateMch_>
{
  char* input_stream;
  std::vector<char*>* tokens;
  std::vector<int> has_esc{128};
  size_t token_idx;
  size_t escape_idx;
  char* input_cur_location;
  char* start_token;
  bool end_of_parse;

  typedef csvStateMch_ csv_rules;

  csvStateMch_():end_of_parse(false) {}

  void set(const char* input, std::vector<char*>* tk)
  {
    input_cur_location = input_stream = const_cast<char*>(input);
    token_idx = 0;
    tokens = tk;
    escape_idx = 0;
  }

  char get_char()
  {
    return *input_cur_location;
  }


  char get_next_char(const char* end_stream)
  {
    input_cur_location++;

    if(input_cur_location >= end_stream)
    {
      return 0;
    }

    return *input_cur_location;
  }

  const char* currentLoc()
  {
    return input_cur_location;
  }

  void parse_escape(char* in, char esc_char='\\')
  {
    //assumption atleast one escape and single
    char* dst, *src;

    dst = src = in;

    while (1)
    {
      while (*src && *src != esc_char)
      {
        src++;  //search for escape
      }

      if (!*src) //reach end
      {
        char* p = src;
        while (dst < src)
        {
          *dst++ = *p++;  //full copy
        }
        return;
      }
      //found escape
      dst = src; //override escape
      //if(*(dst+1)=='n') {*dst=10;dst++;} //enables special character

      while (*dst)
      {
        *dst = *(dst + 1);
        dst++;
      } //copy with shift
    }
  }

  // The list of FSM states
  struct Start_new_token_st : public msm::front::state<>
  {};//0

  struct In_new_token_st : public msm::front::state<>
  {};//1

  struct In_quote_st : public msm::front::state<>
  {};//2

  struct In_esc_in_token_st : public msm::front::state<>
  {};//3

  struct In_esc_quote_st : public msm::front::state<>
  {};//4

  struct In_esc_start_token_st : public msm::front::state<>
  {};//5

  struct End_of_line_st : public msm::front::state<>
  {};//6

  struct Empty_state : public msm::front::state<>
  {};//7


  // the initial state of the csvStateMch SM. Must be defined
  typedef Start_new_token_st initial_state;

  void start_new_token()//helper
  {
    start_token = input_cur_location;
    (*tokens)[ token_idx ] = start_token;
    token_idx++;
  }

  // transition actions
  void start_new_token(event_column_sep const&)
  {
    *input_cur_location = 0;//remove column-delimiter
    start_new_token();
  }

  void start_new_token(event_not_column_sep const&)
  {
    start_new_token();
  }

  //need to handle empty lines(no tokens);
  void start_new_token(event_eol const&)
  {
    if(!token_idx)
    {
      return;
    }
    (*tokens)[ token_idx ] = start_token;
    token_idx++;
  }

  void start_new_token(event_end_of_stream const&) {}

  void in_new_token(event_not_column_sep const&)
  {
    if(!*start_token)
    {
      start_token = input_cur_location;
    }
  }

  void in_new_token(event_eol const&)
  {
    *input_cur_location=0;
  }

  void in_new_token(event_end_of_stream const&) {}

  void in_new_token(event_column_sep const&)
  {
    (*tokens)[ token_idx ] = input_cur_location+1;
    *input_cur_location=0;
    token_idx++;
  }

  void in_new_token(event_quote const&)
  {
    if(!*start_token)
    {
      start_token = input_cur_location;
    }
  }

  void in_quote(event_quote const&) {}

  void in_quote(event_column_sep const&) {}

  void in_quote(event_not_column_sep const&) {}

  void in_quote(event_eol const&)
  {
    *input_cur_location=0;
  }

  void in_quote(event_end_of_stream const&)
  {
    *input_cur_location=0;
  }

  void start_new_token(event_quote const&)
  {
    start_new_token();
  }

  void push_escape_pos()
  {
    if(escape_idx && has_esc[ escape_idx -1]== (int)(token_idx-1))
    {
      return;
    }
    has_esc[ escape_idx ] = token_idx-1;
    escape_idx++;
  }
  void in_escape(event_escape const&)
  {
    push_escape_pos();
  }
  void in_escape_start_new_token(event_escape const&)
  {
    start_new_token();
    push_escape_pos();
  }

  void in_escape(event_column_sep const&) {}
  void in_escape(event_not_column_sep const&) {}
  void in_escape(event_quote const&) {}
  void in_escape(event_eol const&) {}
  void in_escape(event_end_of_stream const&) {}

  void empty_action(event_empty const&) {}

  //TODO need a guard for tokens vector size (<MAX)
  // Transition table for csvStateMch
  struct transition_table : mpl::vector30<
  //           Start     		Event         		      Next      Action		 Guard
  //  +---------+-------------+---------+---------------------+----------------------+
    a_row < Start_new_token_st, event_column_sep 	, Start_new_token_st 	, &csv_rules::start_new_token      >,
    a_row < Start_new_token_st, event_not_column_sep 	, In_new_token_st 	, &csv_rules::start_new_token      >,
    a_row < Start_new_token_st, event_eol 	, End_of_line_st 	, &csv_rules::start_new_token      >,
    a_row < Start_new_token_st, event_end_of_stream 	, End_of_line_st 	, &csv_rules::start_new_token      >,
    a_row < In_new_token_st 	, event_not_column_sep, In_new_token_st, &csv_rules::in_new_token    >,
    a_row < In_new_token_st 	, event_column_sep  	, In_new_token_st, &csv_rules::in_new_token    >,
    a_row < In_new_token_st 	, event_eol  		, End_of_line_st, &csv_rules::in_new_token    >,
    a_row < In_new_token_st 	, event_end_of_stream  	, End_of_line_st, &csv_rules::in_new_token    >,

    a_row < Start_new_token_st 	, event_quote  	, In_quote_st, &csv_rules::start_new_token    >,   //open quote
    a_row < In_new_token_st 	, event_quote  	, In_quote_st, &csv_rules::in_quote    >,   //open quote
    a_row < In_quote_st 	, event_quote  	, In_new_token_st, &csv_rules::in_quote    >,   //close quote
    a_row < In_quote_st 	, event_column_sep  	, In_quote_st, &csv_rules::in_quote    >,   //stay in quote
    a_row < In_quote_st 	, event_not_column_sep  	, In_quote_st, &csv_rules::in_quote    >,   //stay in quote
    a_row < In_quote_st 	, event_eol  	, End_of_line_st, &csv_rules::in_quote    >,   //end of quote/line
    a_row < In_quote_st 	, event_end_of_stream  	, End_of_line_st, &csv_rules::in_quote    >,   //end of quote/line


  //TODO add transitions for escape just before eol , eos.
    a_row < Start_new_token_st 	, event_escape  	, In_esc_start_token_st, &csv_rules::in_escape_start_new_token    >,
    a_row < In_esc_start_token_st, event_column_sep, In_new_token_st, &csv_rules::in_escape    >,      //escape column-sep
    a_row < In_esc_start_token_st, event_not_column_sep, In_new_token_st, &csv_rules::in_escape    >,
    a_row < In_esc_start_token_st, event_escape, In_new_token_st, &csv_rules::in_escape    >,
    a_row < In_esc_start_token_st, event_quote, In_new_token_st, &csv_rules::in_escape    >,

    a_row < In_new_token_st, event_escape, In_esc_in_token_st, &csv_rules::in_escape    >,
    a_row < In_esc_in_token_st, event_column_sep, In_new_token_st, &csv_rules::in_escape    >,
    a_row < In_esc_in_token_st, event_not_column_sep, In_new_token_st, &csv_rules::in_escape    >,
    a_row < In_esc_in_token_st, event_escape, In_new_token_st, &csv_rules::in_escape    >,
    a_row < In_esc_in_token_st, event_quote, In_new_token_st, &csv_rules::in_escape    >,

    a_row < In_quote_st, event_escape, In_esc_quote_st, &csv_rules::in_escape    >,
    a_row < In_esc_quote_st, event_column_sep, In_quote_st, &csv_rules::in_escape    >,
    a_row < In_esc_quote_st, event_not_column_sep, In_quote_st, &csv_rules::in_escape    >,
    a_row < In_esc_quote_st, event_escape, In_quote_st, &csv_rules::in_escape    >,
    a_row < In_esc_quote_st, event_quote, In_quote_st, &csv_rules::in_escape    >

  //  +---------+-------------+---------+---------------------+----------------------+
    > {};

  // Replaces the default no-transition response.
  template <class FSM, class Event>
  void no_transition(Event const& e, FSM&, int state)
  {
    std::cout << "no transition from state " << state
              << " on event " << typeid(e).name() << std::endl;
  }
}; //// end-of-state-machine



// Pick a back-end
typedef msm::back::state_machine<csvStateMch_> csvStateMch;

//
// Testing utilities.
//

static char const* const state_names[] = {"Start_new_token_st", "In_new_token_st", "In_quote_st", "In_esc_in_token_st",
                                          "In_esc_quote_st", "In_esc_start_token_st", "End_of_line_st", "Empty_state"
                                         };
void pstate(csvStateMch const& p)//debug
{
  std::cout << " -> " << state_names[p.current_state()[0]] << std::endl;
}


class csvParser
{

  csvStateMch p;

  char m_row_delimeter;
  char m_column_delimiter;
  char m_quote_char;
  char m_escape_char;

public:

  csvParser(char rd='\n', char cd=',', char quot='"', char ec='\\'):m_row_delimeter(rd), m_column_delimiter(cd), m_quote_char(quot), m_escape_char(ec) {};

  void set(char row_delimiter, char column_delimiter, char quot_char, char escape_char)
  {
    m_row_delimeter = row_delimiter;
    m_column_delimiter = column_delimiter;
    m_quote_char = quot_char;
    m_escape_char = escape_char;
  }

  int parse(char* input_stream, char* end_stream, std::vector<char*>* tk, size_t* num_of_tokens)
  {
    p.set(input_stream, tk);

    // needed to start the highest-level SM. This will call on_entry and mark the start of the SM
    p.start();

    //TODO for better performance to use template specialization (\n  \ , ")
    do
    {
      if (p.currentLoc() >= end_stream)
      {
        break;
      }

      if (p.get_char() == m_row_delimeter)
      {
        p.process_event(event_eol());
      }
      else if (p.get_char() == m_column_delimiter)
      {
        p.process_event(event_column_sep());
      }
      else if (p.get_char() == 0)
      {
        p.process_event(event_end_of_stream());
      }
      else if (p.get_char() == m_quote_char)
      {
        p.process_event(event_quote());
      }
      else if (p.get_char() == m_escape_char)
      {
        p.process_event(event_escape());
      }
      else
      {
        p.process_event(event_not_column_sep());
      }

      if (p.tokens->capacity() <= p.token_idx)
      {
        return -1;
      }

      p.get_next_char(end_stream);

    }
    while (p.current_state()[0] != 6);

    p.stop();

    *num_of_tokens = p.token_idx;

    //second pass for escape rules; only token with escape are processed, if any.
    for(size_t i=0; i<p.escape_idx; i++)
    {
      p.parse_escape((*tk)[p.has_esc[i]], m_escape_char);
    }

    return 0;
  }

  const char* currentLoc()
  {
    return p.currentLoc();
  }

};//end csv-parser

}//end-namespace


