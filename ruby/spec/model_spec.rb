###
### $Rev$
### $Release: $
### $Copyright$
### $License$
###

File.class_eval do
  path = join(dirname(dirname(expand_path(__FILE__))), 'lib')
  $:.unshift path
  path = join(dirname(dirname(expand_path(__FILE__))), 'spec')
  $:.unshift path
end


require 'kwery'
require 'kwery/adapters/mysql'
require 'database_config'


conn = Kwery.connect(HOST, USER, PASS, DBNAME)
q = Kwery::Query.new(conn)
#q.output = $output
now = :current_timestamp


class Team
  include Kwery::Model
  create_table('teams') do |t|
    t.integer(:id)  {|c| c.primary_key.serial }
    t.string(:name) {|c| c.not_null.unique }
    t.text(:desc)
    t.integer(:owner_id) {|c| c.references('members') }
    t.timestamp(:created_at) {|c| c.not_null.default(:current_timestamp) }
    t.timestamp(:updated_at) {|c| c.not_null }
    t.boolean(:deleted) {|c| c.not_null.default(false) }
  end
  attr_accessor :owner
end

class Member
  include Kwery::Model
  create_table('members') do |t|
    t.integer(:id) {|c| c.primary_key.serial }
    t.string(:name, 64) {|c| c.not_null }
    t.text(:desc)
    t.integer(:team_id) {|c| c.not_null.references('teams') }
    t.date(:birth)
    t.timestamp(:created_at) {|c| }
    t.timestamp(:updated_at) {|c| c.not_null.default(:current_timestamp) }
    t.boolean(:deleted) {|c| c.not_null.default(false) }
  end
  attr_accessor :team
end


describe 'Kwery::Model.create_table' do

  q.execute("drop table if exists #{Team.__table__}")
  q.execute("drop table if exists #{Member.__table__}")

  col_names1 = [:id, :name, :desc, :owner_id, :created_at, :updated_at, :deleted]
  col_names2 = [:id, :name, :desc, :team_id, :birth, :created_at, :updated_at, :deleted]

  it "sets @__columns___ to list of Columns." do
    Team.__columns__.should be_a_kind_of(Array)
    Team.__columns__.length.should == 7
    Team.__columns__.each {|x| x.should be_a_kind_of(Kwery::Column) }
    Team.__columns__.collect {|x| x._name }.should == col_names1
    Member.__columns__.should be_a_kind_of(Array)
    Member.__columns__.length.should == 8
    Member.__columns__.each {|x| x.should be_a_kind_of(Kwery::Column) }
    Member.__columns__.collect {|x| x._name }.should == col_names2
  end

  it "sets @__column_names__ to list of symbol" do
    Team.__column_names__.should == col_names1
    Member.__column_names__.should == col_names2
  end

  it "defines accessors" do
    arr = Team.instance_methods.collect {|x| x.to_sym }
    col_names1.all? {|x| arr.include?(x) }.should == true
    col_names1.all? {|x| arr.include?((x.to_s+'=').to_sym) }.should == true
  end

end


describe 'Kwery::Model.to_sql' do

  it "returns create tabel sql." do
    sql = Team.to_sql(q)
    expected = <<END
create table teams (
  id                 integer         primary key auto_increment,
  name               varchar(255)    not null unique,
  `desc`             text           ,
  owner_id           integer         references members(id),
  created_at         timestamp       not null default current_timestamp,
  updated_at         timestamp       not null default 0,
  deleted            boolean         not null default false
)
END
    sql.should == expected.chomp
    #
    sql = Member.to_sql
    expected = <<END
create table members (
  id                 integer         primary key auto_increment,
  name               varchar(64)     not null,
  `desc`             text           ,
  team_id            integer         not null references teams(id),
  birth              date           ,
  created_at         timestamp       null default null,
  updated_at         timestamp       not null default current_timestamp,
  deleted            boolean         not null default false
)
END
    sql.should == expected.chomp
  end

  it "builds unique constrants which has several column names." do
    class Taggings
      include Kwery::Model
      create_table('taggings') do |t|
        t.integer(:id) {|c| c.primary_key.serial }
        t.integer(:post_id) {|c| c.not_null.references('posts') }
        t.integer(:tag_id) {|c| c.not_null.references('tags') }
        t.integer(:category_id) {|c| c.not_null.references('categories') }
        t.unique(:tag_id, :post_id)  # !!!
        t.unique(:category_id, :post_id)  # !!!
      end
    end
    expected = <<END
create table taggings (
  id                 integer         primary key auto_increment,
  post_id            integer         not null references posts(id),
  tag_id             integer         not null references tags(id),
  category_id        integer         not null references categories(id),
  unique(tag_id, post_id),
  unique(category_id, post_id)
)
END
    expected.chomp!
    Taggings.to_sql.should == expected
  end

end


## create table
q.execute(Team.to_sql(q))
q.execute(Member.to_sql(q))



sos = ryouou = nil

describe "Kwery::Model#__insert__" do

  it "sets insert id automatically." do
    sos = Team.new(nil, 'sos', 'SOS Brigate', nil, nil, nil, false)
    q.insert(sos)
    sos.id.should == 1
    ryouou = Team.new(:name=>'ryouou', :deleted=>false)
    q.insert(ryouou)
    ryouou.id.should == 2
    #
    q.insert(haruhi = Member.new(:name=>'Haruhi', :team_id=>sos.id, :deleted=>false))
    q.insert(mikuru = Member.new(:name=>'Mikuro', :team_id=>sos.id, :deleted=>false))
    q.insert(yuki   = Member.new(:name=>'Yuki', :team_id=>sos.id, :deleted=>false))
    q.get_all(Member).length.should == 3
    haruhi.id.should == 1
    mikuru.id.should == 2
    yuki.id.should == 3
  end

  it "raises error when selected model object." do
    obj = q.get(Team, sos.id)
    proc { q.insert(obj) }.should raise_error(RuntimeError, 'Already inserted.')
  end

  it "sets created_at and updated_at column automatically." do
    sos = q.get(Team, sos.id)
    sos.created_at.should be_a_kind_of(Kwery::TIMESTAMP_CLASS)
    sos.updated_at.should be_a_kind_of(Kwery::TIMESTAMP_CLASS)
    ryouou = q.get(Team, ryouou.id)
    ryouou.created_at.should be_a_kind_of(Kwery::TIMESTAMP_CLASS)
    ryouou.updated_at.should be_a_kind_of(Kwery::TIMESTAMP_CLASS)
  end

end


describe "Kwery::Model#__update__" do

  it "updates only changed columns" do
    ryouou.desc.should == nil
    desc = 'Ryouou Hight School'
    ryouou.desc = desc
    q.output = ''
    q.update(ryouou)
    q.output.should == "update teams set updated_at=current_timestamp, `desc`='Ryouou Hight School' where id = 2\n"
    q.output = nil
    q.get(Team, ryouou.id).desc.should == desc
  end

  it "raises error when non-inserted object" do
    team = Team.new(:name=>'Saitama', :deleted=>false)
    proc { q.update(team) }.should raise_error(RuntimeError, 'Not inserted object.')
  end

  it "sets updated_at column automatically" do
    haruhi = q.get(Member) {|b| b.where(:name, 'Haruhi') }
    haruhi.should_not == nil
    created_at = haruhi.created_at
    updated_at = haruhi.updated_at
    updated_at.should == created_at
    sleep(1)
    haruhi.desc = 'Haruhi Suzumiya'
    q.update(haruhi)
    haruhi = q.get(Member, haruhi.id)
    haruhi.created_at.should == created_at
    haruhi.updated_at.should_not == updated_at
    #haruhi.updated_at.should be_larger_than(updated_at)
    #haruhi.updated_at.should satisfy {|t| t > updated_at }
  end

end


describe "Kwery::Model.__delete__" do

  it "deletes model object" do
    haruhi = q.get(Member) {|b| b.where(:name, 'Haruhi') }
    haruhi.should_not == nil
    q.delete(haruhi)
    haruhi = q.get(Member) {|b| b.where(:name, 'Haruhi') }
    haruhi.should == nil
  end

  it "raises error when non-inserted object" do
    member = Member.new(:name=>'Minoru', :deleted=>false)
    proc { q.delete(member) }.should raise_error(RuntimeError, 'Not inserted object.')
  end

end
