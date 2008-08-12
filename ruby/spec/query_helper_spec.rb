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


#require 'rubygems'
#require 'rspec'
require 'kwery'
require 'kwery/adapters/mysql'
require 'database_config'


conn = Kwery.connect(HOST, USER, PASS, DBNAME)
q = Kwery::Query.new(conn)
#q.debug = true
now = :current_timestamp


describe 'Kwery::QueryHelper#map_each_ref' do

  create_table_option = ' engine=InnoDB'
  drop_table_option = ' if exists'
  q.execute "drop table #{drop_table_option} teams"
  q.execute <<END
create table teams (
  id         integer       primary key auto_increment,
  name       varchar(255)  not null unique,
  `desc`     text,
  owner_id   integer       references members(id),
  created_at timestamp     not null,
  updated_at timestamp     not null
) #{create_table_option}
END
  q.execute "drop table #{drop_table_option} members"
  q.execute <<END
create table members (
  id         integer       primary key auto_increment,
  name       varchar(255)  not null,
  `desc`     text,
  team_id    integer       references teams(id),
  created_at timestamp     not null,
  updated_at timestamp     not null
) #{create_table_option}
END
  end

end


describe 'Kwery::Query#insert' do

  it "inserts hash data" do
    q.insert('teams', {:name=>'sos', :desc=>'SOS Brigate'})
    id = q.last_insert_id
    #
    q.insert('members', {:name=>'Haruhi', :team_id=>id, :created_at=>now, :updated_at=>now})
    q.insert('members', {:name=>'Mikuru', :team_id=>id, :created_at=>Time.now, :updated_at=>Time.now})
    q.insert('members', {:name=>'Yuki', :team_id=>id, :created_at=>:'now()', :updated_at=>:'now()'})
    #
    q.select('teams').length.should == 1
    q.select('members').length.should == 3
  end

  it "inserts array data" do
    q.insert('teams', [nil, 'ryouou', 'Ryouou Gakuen High School', nil, now, now])
    id = q.last_insert_id
    #
    q.insert('members', [nil, 'Konata',  nil, id, now, now])
    q.insert('members', [nil, 'Kagami',  nil, id, now, now])
    q.insert('members', [nil, 'Tsukasa', nil, id, now, now])
    q.insert('members', [nil, 'Miyuki',  nil, id, now, now])
    #
    q.select('teams').length.should == 2
    q.select('members').length.should == 7
  end

end


describe 'Kwery::Query#update' do

  it "updates data with conditions" do
    desc = "Haruhi's brigate"  # contains "'"
    q.update('teams', {:desc=>desc, :updated_at=>Time.now}) {|c| c.where('name = ', 'sos') }
    hash = q.get('teams', 1)
    hash['desc'].should == desc
  end

  it "updates data with id" do
    mikuru = q.get('members') {|c| c.where(:name, 'Mikuru') }
    mikuru['desc'].should == nil
    desc = 'Future-woman'
    q.update('members', {:desc=>desc, :updated_at=>now}, mikuru['id'])
    mikuru = q.get('members', mikuru['id'].to_i)
    mikuru['desc'].should == desc
  end

  it "throws ArugmentError when condition is not specified" do
    msg = 'update condition is reqiured.'
    proc { q.update('teams', {:owner_id=>1}) }.should raise_error(ArgumentError, msg)
  end

end


describe 'Kwery::Query#get' do

  it "returns a Hash object" do
    hash = q.get('members', 1)
    hash.should be_kind_of(Hash)
    hash['id'].to_i.should == 1
    hash['name'].should == 'Haruhi'
  end

  it "can take a block" do
    hash = q.get('members') {|c| c.where(:id, 1) }
    hash.should be_kind_of(Hash)
    hash['id'].to_i.should == 1
    hash['name'].should == 'Haruhi'
  end

  it "returns nil when data not found" do
    hash = q.get('members', 999)
    hash.should == nil
    hash = q.get('members') {|c| c.where(:name, 'Minoru') }
    hash.should == nil
  end

end


describe 'Kwery::Query#get_all' do

  it "returns an Array of Hash object" do
    list = q.get_all('members')
    list.should be_a_kind_of(Array)
    list.length.should == 7
    list.first.should be_a_kind_of(Hash)
  end

  it "can take a block" do
    list = q.get_all('members') {|c| c.where(:team_id, 1) }
    list.should be_a_kind_of(Array)
    list.first.should be_a_kind_of(Hash)
    list.length.should == 3
    list.each {|hash| hash['team_id'].to_i.should == 1 }
  end

  it "can take where-clause" do
    expected = q.get_all('members') {|c| c.where(:team_id, 1) }
    q.get_all('members', 'team_id = 1').should == expected
    q.get_all('members', :team_id, 1).should == expected
    q.get_all('members', 'team_id =', 1).should == expected
    q.get_all('members', :team_id => 1).should == expected
    #q.get_all('members', [[:team_id, 1]]).should == expected
  end

  it "returns empty Array when data not found" do
    list = q.get_all('members') {|c| c.where(:team_id, 999) }
    list.should be_a_kind_of(Array)
    list.length.should == 0
  end

end


describe 'Kwery::Query#select' do

  it "returns enumerable" do
    enum = q.select('members')
    enum.should be_a_kind_of(Enumerable)
    arr = enum.to_a
    arr.length.should == 7
  end

  it "can take a block" do
    enum = q.select('members') {|c| c.where(:team_id, 1) }
    arr = enum.to_a
    arr.collect {|e| e['name'] }.should == ['Haruhi', 'Mikuru', 'Yuki']
  end

  it "can take column names" do
    enum = q.select('members', 'id, name') {|c| c.where(:team_id, 1) }
    arr = enum.to_a
    arr.each {|h| h['id'] = h['id'].to_i }
    arr.should == [{'id'=>1, 'name'=>'Haruhi'}, {'id'=>2, 'name'=>'Mikuru'}, {'id'=>3, 'name'=>'Yuki'}]
  end

  class Member1
    attr_accessor :id, :name, :desc, :team_id, :created_at, :updated_at, :deleted
  end

  it "can take class object" do
    enum = q.select('members', nil, Hash) {|c| c.where(:team_id, 1) }
    enum.to_a[0].should be_a_kind_of(Hash)
    enum = q.select('members', nil, Array) {|c| c.where(:team_id, 1) }
    enum.to_a[0].should be_a_kind_of(Array)
    enum.to_a[0][1].should == 'Haruhi'
    enum = q.select('members', nil, Member1) {|c| c.where(:team_id, 1) }
    enum.to_a[0].should be_a_kind_of(Member1)
    yuki = enum.to_a[2]
    yuki.name.should == 'Yuki'
    yuki.team_id.to_i.should == 1
  end

  expected = <<END
1, Haruhi, 1, sos
2, Mikuru, 1, sos
3, Yuki, 1, sos
4, Konata, 2, ryouou
5, Kagami, 2, ryouou
6, Tsukasa, 2, ryouou
7, Miyuki, 2, ryouou
END

  it "can take several table names" do
    columns = 'members.*, teams.name team_name'
    enum = q.select('members, teams', columns) {|c| c.where('members.team_id = teams.id').order_by('members.id') }
    arr = enum.to_a
    #collected = arr.collect {|e| [e['id'], e['name'], e['team_id'], e['team_name']].join(", ") }
    collected = arr.collect {|e| e.values_at(*%w[id name team_id team_name]).join(", ") }
    result = collected.join("\n") << "\n"
    result.should == expected
  end

  it "supports table name aliases" do
    columns = 'm.*, t.name team_name'
    enum = q.select('members m, teams t', columns) {|c| c.where('m.team_id = t.id').order_by('m.id') }
    arr = enum.to_a
    #collected = arr.collect {|e| [e['id'], e['name'], e['team_id'], e['team_name']].join(", ") }
    collected = arr.collect {|e| e.values_at(*%w[id name team_id team_name]).join(", ") }
    result = collected.join("\n") << "\n"
    result.should == expected
  end

  it "supports join" do
    haruhi_id = 1
    q.update('teams', {:owner_id=>haruhi_id}) {|c| c.where(:name, 'sos') }
    q.update('teams', {:owner_id=>nil}) {|c| c.where(:name, 'ryouou') }
    #
    enum = q.select('teams, members') {|c| c.where('teams.owner_id = members.id').order_by('teams.id') }
    arr = enum.to_a
    arr.length.should == 1
    #
    columns = 'teams.*, members.name owner_name'
    enum = q.select('teams', columns) {|c| c.left_outer_join('members', :owner_id).order_by('teams.id') }
    arr = enum.to_a
    arr.length.should == 2
    collected = arr.collect {|e| e.values_at(*%w[id name owner_id owner_name]).join(', ') }
    actual = collected.join("\n") << "\n"
    expected = <<END
1, sos, 1, Haruhi
2, ryouou, , 
END
    actual.should == expected
  end

  it "supports join with specifying class object" do
    columns = 'teams.*, members.name owner_name'
    enum = q.select('teams', columns, Array) {|c| c.left_outer_join('members', :owner_id).order_by('teams.id') }
    arr = enum.to_a
    arr.length.should == 2
    arr[0].should be_a_kind_of(Array)
  end

end


describe 'Kwery::Query#select_only' do

  it "returns an array of values, not hashes" do
    q.select_only('teams', :name).should == ['ryouou', 'sos']
    arr = q.select_only('members', :name) {|c| c.where(:team_id, 2).order_by(:name) }
    arr.should == ['Kagami', 'Konata', 'Miyuki', 'Tsukasa']
  end

end


describe 'Kwery::Query#transaction' do

  it "commits when no errors" do
    kagami = q.get('members') {|c| c.where(:name, 'Kagami') }
    id = kagami['id']
    s = 'Hiiragi Kagami'
    q.transaction do
      q.update('members', {:desc=>s}, id)
    end
    q.get('members', id)['desc'].should == s
  end

  it "rollbacks when error raised" do
    tsukasa = q.get('members') {|c| c.where(:name, 'Tsukasa') }
    id = tsukasa['id']
    s = 'Yo-ni-ge de reset'
    proc {
      q.transaction do
        proc {
          q.update('members', {:desc=>s}, id)
          q.get('members', id)['desc'].should == s
        }.should_not raise_error(Exception)
        q.update('members', {:description=>s}, id)  # error
      end
    }.should raise_error(Kwery::SQL_ERROR_CLASS)  # Unknown column 'description' in 'field list'
    q.get('members', id)['desc'].should_not == s
    q.get('members', id)['desc'].should == tsukasa['desc']
  end

end


describe 'Kwery::Query#delete' do

  it "deletes data specified by id" do
    #q.debug = true
    miyuki = q.get('members') {|c| c.where('name', 'Miyuki') }
    q.delete('members', miyuki['id'])
    q.get('members') {|c| c.where('name', 'Miyuki') }.should == nil
    q.get_all('members').length.should == 6
  end

  it "deletes data specified by block" do
    #q.debug = true
    ryouou = q.get('teams') {|c| c.where('name', 'ryouou') }
    id = ryouou['id']
    q.delete('members') {|c| c.where('team_id', id) }
    q.get_all('members').length.should == 3
    q.get_all('members') {|c| c.where('team_id', id) }.length.should == 0
  end

  it "throws ArugmentError when condition is not specified" do
    msg = 'delete condition is reqiured.'
    proc { q.delete('teams') }.should raise_error(ArgumentError, msg)
  end

end


describe "Kwery::Qwery#delete_all" do

  it "deletes all data" do
    q.delete_all('members')
    q.get_all('members').length.should == 0
  end

end


describe "Kwery::Qwery#last_insert_id" do

  it "returns last insert id" do
    q.insert('members', :name=>'Yutaka')
    id = q.last_insert_id
    id.should be_a_kind_of(Fixnum)
    #id.should == 7
    q.insert('members', :name=>'Minami')
    q.last_insert_id.should == (id+1)
  end

end
