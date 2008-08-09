###
### $Rev$
### $Release: $
### $Copyright$
### $License$
###


require 'kwery/query'
require 'mysql'


module Kwery


  class MySQLQuery < Query

    MYSQL_KEYWORDS = %w[
          add all alter analyze and as asc asensitive
          before between bigint binary blob both by
          call cascade case change char character check collate column
          condition connection constraint continue convert create cross
          current_date current_time current_timestamp current_user cursor
          database databases day_hour day_microsecond day_minute day_second
          dec decimal declare default delayed delete desc describe
          deterministic distinct distinctrow div double drop dual
          each else elseif enclosed escaped exists exit explain
          false fetch float for force foreign from fulltext
          goto grant group
          having high_priority hour_microsecond hour_minute hour_second
          if ignore in index infile inner inout insensitive insert
          int integer interval into is iterate
          join
          key keys kill
          leading leave left like limit lines load localtime
          localtimestamp lock long longblob longtext loop low_priority
          match mediumblob mediumint mediumtext middleint
          minute_microsecond minute_second mod modifies
          natural not no_write_to_binlog null numeric
          on optimize option optionally or order out outer outfile
          precision primary procedure purge
          read reads real references regexp release rename repeat
          replace require restrict return revoke right rlike
          schema schemas second_microsecond select sensitive
          separator set show smallint soname spatial specific sql
          sqlexception sqlstate sqlwarning sql_big_result
          sql_calc_found_rows sql_small_result ssl starting straight_join
          table terminated then tinyblob tinyint tinytext to
          trailing trigger true
          undo union unique unlock unsigned update usage use using
          utc_date utc_time utc_timestamp
          values varbinary varchar varcharacter varying
          when where while with write
          xor
          year_month
          zerofill
    ]

    table = {}
    MYSQL_KEYWORDS.each {|w| table[w] = w; table[w.intern] = w }
    MYSQL_KEYWORD_TABLE = table.freeze

    def escape_string(str)
      #return @conn.escape_string(str)
      return str.gsub(/'/, "\\\\\'")
    end

    def quote_keyword(word)
      return MYSQL_KEYWORD_TABLE[word] ? "`#{word}`" : word
    end

    def execute(sql)
      $stderr.puts sql if @debug
      #return @conn.query(sql)
      stmt = @conn.prepare(sql)
      stmt.execute()
      meta = stmt.result_metadata()
      field_names = []
      if meta
        while field = meta.fetch_field
          #field_names << field.name.intern
          field_names << field.name
        end
        meta.free()
      end
      return MySQLResult.new(field_names, stmt)
    end

    def start_transaction   ## prepared statement doesn't support transaction
      return @conn.query('start transaction')
    end

    def commit
      return @conn.query('commit')
    end

    def rollback
      return @conn.query('rollback')
    end

  end


  class MySQLResult

    def initialize(column_names, stmt)
      @column_names = column_names
      @stmt = stmt
    end

    def fetch_array
      return @stmt.fetch()
    end

    def fetch_hash
      arr = @stmt.fetch()
      return nil unless arr
      hash = {}
      @column_names.zip(arr) {|key, val| hash[key] = val }
      return hash
    end

    def each_array
      while arr = @stmt.fetch()
        yield(arr)
      end
      nil
    end

    def each_hash
      while arr = @stmt.fetch()
        hash = {}
        @column_names.zip(arr) {|key, val| hash[key] = val }
        yield(hash)
      end
      nil
    end

    def free
      @column_names = nil
      @stmt.free_result()
    end

  end


  class Query

    def self.new(*args)
      return MySQLQuery.__new__(*args)  # Query.new returns MySQLQuery object
    end

  end


  def self.connect(host=nil, user=nil, passwd=nil, dbname=nil, options=nil)
    return ::Mysql.connect(host||'loalhost', user||'root', passwd, dbname)
  end


  SQL_ERROR_CLASS = ::Mysql::Error


end


class ::Mysql::Result

  alias each_array  each
  alias fetch_array fetch_row
  ## each_hash and fetch_hash are already defined

end
