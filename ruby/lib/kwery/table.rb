###
### $Rev: $
### $Release: $
### $Copyright$
### $License$
###

module Kwery

  module ColumnBuilder

    def integer(name, max=nil)
      @_max = max
      yield(self) if block_given?
      @_columns << build_integer_column(name, max)
    end

    def build_integer_column(name, nax)
      
    end

  end


  module TableBuilder

    def initialize(conn=nil)
      @conn = conn
    end
    attr_accessor :conn, :prefix

    def create_table(table_name, opts={})
      @before_block.call(self) if @before_block
      yield(self)
      @after_block.call(self)  if @after_block
      #
      sql = build_create_table_sql()
      if (@conn)
        
      end
      @_columns
    end

    def before(&block)
      @before_block = block
    end

    def after(&block)
      @after_block = block
    end

  end


  class Table

    def create
    end
  end
  

  q.create_table('teams') do |t|
    #t.add(:id)
    t.integer(:id) {|f| f.primary_key.auto_increment }
    t.string(:name, 255) {|f| f.not_null.unique }
    t.text(:desc)
    #t.add(:created_at)
    #t.add(:updated_at)
    t.timestamp(:created_at) {|f| f.not_null }
    t.timestamp(:updated_at) {|f| f.not_null.on_update(:current_timestamp) }
    t.boolean(:delete) {|f| f.default(false) }
  end

end
