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
  set_table_name 'teams'
  add_columns :id, :name, :desc, :owner_id, :created_at, :updated_at
  attr_accessor :owner
end

class Member
  include Kwery::Model
  set_table_name 'members'
  add_columns :id, :name, :desc, :team_id, :created_at, :updated_at
  attr_accessor :team
end


describe 'Kwery::QueryExecutor#execute' do

  create_table_option = ' engine=InnoDB'
  drop_table_option = ' if exists'
  it "executes create table sql statement" do
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


describe 'Kwery::QueryExecutor#insert' do

  sos_id = nil
  it "inserts hash data" do
    q.insert('teams', {:name=>'sos', :desc=>'SOS Brigate'})
    sos_id = q.last_insert_id
    #
    q.insert('members', {:name=>'Haruhi', :team_id=>sos_id, :created_at=>now, :updated_at=>now})
    #q.insert('members', {:name=>'Mikuru', :team_id=>sos_id, :created_at=>Time.now, :updated_at=>Time.now})
    #q.insert('members', {:name=>'Yuki', :team_id=>sos_id, :created_at=>:'now()', :updated_at=>:'now()'})
    #
    q.select('teams').length.should == 1
    q.select('members').length.should == 1
  end

  it "can take Model class" do
    q.insert(Member, {:name=>'Mikuru', :team_id=>sos_id, :created_at=>Time.now, :updated_at=>Time.now})
    q.insert(Member, {:name=>'Yuki', :team_id=>sos_id, :created_at=>:'now()', :updated_at=>:'now()'})
    q.select('members').length.should == 3
  end

  ryouou_id = nil
  it "inserts array data" do
    q.insert('teams', [nil, 'ryouou', 'Ryouou Gakuen High School', nil, now, now])
    ryouou_id = q.last_insert_id
    #
    q.insert('members', [nil, 'Konata',  nil, ryouou_id, now, now])
    q.insert('members', [nil, 'Kagami',  nil, ryouou_id, now, now])
    #q.insert('members', [nil, 'Tsukasa',  nil, ryouou_id, now, now])
    #q.insert('members', [nil, 'Miyuki',  nil, ryouou_id, now, now])
    #
    q.select('teams').length.should == 2
    q.select('members').length.should == 5
  end

  it "can talke model object" do
    tsukasa = Member.new(nil, 'Tsukasa', nil, ryouou_id, Time.now, ':now()')
    q.insert_object(tsukasa)
    miyuki = Member.new(:name=>'Miyuki', :team_id=>ryouou_id)
    q.insert_object(miyuki)
    q.select('members').length.should == 7
  end

end


describe 'Kwery::QueryExecutor#update' do

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

  it "throws ArgugmentError when condition is not specified" do
    msg = 'update condition is reqiured.'
    proc { q.update('teams', {:owner_id=>1}) }.should raise_error(ArgumentError, msg)
  end

  it "can take Model class" do
    miyuki = q.get(Member) {|c| c.where('name', 'Miyuki') }
    miyuki.desc.should == nil
    miyuki.desc = 'Miyukichi'
    q.update(Member, {:desc=>miyuki.desc}, miyuki.id)
    q.get('members', miyuki.id)['desc'].should == miyuki.desc
  end

  it "can take model object" do
    tsukasa = q.get(Member) {|c| c.where('name', 'Tsukasa') }
    tsukasa.desc.should == nil
    tsukasa.desc = 'Barusamikosu'
    q.update(tsukasa)
    q.get('members', tsukasa.id)['desc'].should == tsukasa.desc
  end

end


describe 'Kwery::QueryExecutor#get' do

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
    #
    obj = q.get(Member, 999)
    obj.should == nil
    obj = q.get(Member) {|c| c.where(:name, 'Minoru') }
    obj.should == nil
  end

  it "can take Model class" do
    sos = q.get(Team, 1)
    sos.should be_a_kind_of(Team)
    sos.name.should == 'sos'
    #
    kagami = q.get(Member) {|c| c.where(:name, 'Kagami') }
    kagami.should be_a_kind_of(Member)
    kagami.name.should == 'Kagami'
  end

end


describe 'Kwery::QueryExecutor#get_all' do

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
    #list.should be_all{|hash| hash['team_id'].to_i == 1 }
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

  it "can take Model class" do
    teams = q.get_all(Team)
    teams.each do |team|
      team.should be_a_kind_of(Team)
      case team['id'].to_i
      when 1 ; team.name == 'sos'
      when 2 ; team.name == 'ryouou'
      end
    end
    #
    ryouou_id = 2
    members = q.get_all(Member) {|c| c.where(:team_id, ryouou_id) }
    members.each do |member|
      member.should be_a_kind_of(Member)
      member.team_id.should == ryouou_id
    end
  end

end


describe 'Kwery::QueryExecutor#select' do

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

  it "can take Model class" do
    t1 = q.to_table_name(Member)
    t2 = q.to_table_name(Team)
    enum = q.select(Member, t1+'.*') {|c| c.left_outer_join(Team, :team_id).where("#{t2}.name", 'ryouou').order_by(t1+'.id') }
    arr = enum.to_a
    arr.length.should == 4
  end

end


describe 'Kwery::QueryExecutor#select_only' do

  it "returns an array of values, not hashes" do
    q.select_only('teams', :name).should == ['ryouou', 'sos']
    arr = q.select_only('members', :name) {|c| c.where(:team_id, 2).order_by(:name) }
    arr.should == ['Kagami', 'Konata', 'Miyuki', 'Tsukasa']
  end

  it "can take Model class" do
    q.select_only(Team, :name) {|c| c.order_by(:name) }.should == ['ryouou', 'sos']
    arr = q.select_only(Member, :name) {|c| c.where(:team_id, 2).order_by(:name) }
    arr.should == ['Kagami', 'Konata', 'Miyuki', 'Tsukasa']
  end

end


describe 'Kwery::QueryHelper#bind_references_to' do

  it "sets reference items to attribute." do
    teams = q.get_all('teams')
    members = q.get_all('members')
    members.each do |member|
      member['team'].should == nil
    end
    q.bind_references_to(members, 'teams', 'team_id', 'team')
    members.each do |member|
      member['team'].should == q.get('teams', member['team_id'])
    end
  end

  it "causes SQL error if null column exists" do
    teams = q.get_all('teams')
    proc {
      q.bind_references_to(teams, 'members', 'owner_id', 'owner')
    }.should raise_error(Exception)   #Kwery::SQL_ERROR_CLASS)
    #q.clear
  end

  it "sets nil when referenced item is not found." do
    teams = q.get_all('teams')
    members = q.get_all('members')
    teams.each do |team|
      team['owner'].should == nil
    end
    q.bind_references_to(teams, 'members', 'owner_id', 'owner', false)
    teams.find {|team| team['name'] == 'sos' }['owner'].should == q.get('members', 1)
    teams.find {|team| team['name'] == 'ryouou' }['owner'].should == nil
  end

  it "can take array of model object" do
    teams = q.get_all(Team)
    teams[0].should be_a_kind_of(Team)
    members = q.get_all(Member)
    members[0].should be_a_kind_of(Member)
    members.each do |member|
      member.team.should == nil
    end
    q.bind_references_to(members, Team, :team_id, :team)
    members.each do |member|
      member.team.id.should == q.get(Team, member.team_id).id
      member.team.name.should == q.get(Team, member.team_id).name
    end
  end

end


describe 'Kwery::QueryHelper#bin_referenced_from' do

  ## multiple=false

  it "sets referenced item to attribute when multiple=false." do
    teams = q.get_all('teams')
    members = q.get_all('members')
    members.each do |member|
      member['team'].should == nil
    end
    members.each {|member| member['owns'].should == nil }
    q.bind_referenced_from(members, 'teams', 'owner_id', 'owns', true, false)
    members.each do |member|
      if member['name'] == 'Haruhi'
        member['owns'].should == q.get('teams') {|c| c.where('name', 'sos') }
      else
        member['owns'].should == nil
      end
    end
  end

  it "can take model objects and Model class when multiple=false." do
    teams = q.get_all(Team)
    members = q.get_all(Member)
    members.each do |member|
      member.team.should == nil
    end
    members.each {|member| member['owns'].should == nil }
    q.bind_referenced_from(members, Team, :owner_id, :owns, true, false)
    members.each do |member|
      if member.name == 'Haruhi'
        haruhi = q.get(Team) {|c| c.where(:name, 'sos') }
        member['owns'].id.should == haruhi.id
        member['owns'].name.should == haruhi.name
      else
        member['owns'].should == nil
      end
    end
  end

  ## multiple=true

  it "sets referenced all items to attribute when multiplue=true" do
    teams = q.get_all('teams')
    members = q.get_all('members')
    teams.each {|team| team['members'].should == nil }
    q.bind_referenced_from(teams, 'members', 'team_id', 'members') {|c| c.order_by(:id) }
    teams.each do |team|
      team['members'].should be_a_kind_of(Array)
      if team['name'] == 'sos'
        team['members'].collect {|x| x['name']}.should == ['Haruhi', 'Mikuru', 'Yuki']
      elsif team['name'] == 'ryouou'
        team['members'].collect {|x| x['name']}.should == ['Konata', 'Kagami', 'Tsukasa', 'Miyuki']
      end
    end
  end

  it "sets empty array when reference data is not found when multiplue=true" do
    teams = q.get_all('teams')
    members = q.get_all('members')
    members.each {|member| member['owns'].should == nil }
    q.bind_referenced_from(members, 'teams', 'owner_id', 'owns')
    members.each do |member|
      if member['name'] == 'Haruhi'
        member['owns'].should be_a_kind_of(Array)
        member['owns'][0]['name'] == 'sos'
      else
        member['owns'].should == []
      end
    end
  end

  it "can take model objects and Model class when multiplue=true" do
    teams = q.get_all(Team)
    members = q.get_all(Member)
    teams.each {|team| team[:members].should == nil }
    q.bind_referenced_from(teams, Member, :team_id, :members) {|c| c.order_by(:id) }
    teams.each do |team|
      team[:members].should be_a_kind_of(Array)
      if team.name == 'sos'
        team[:members].collect {|x| x.name }.should == ['Haruhi', 'Mikuru', 'Yuki']
      elsif team.name == 'ryouou'
        team[:members].collect {|x| x.name }.should == ['Konata', 'Kagami', 'Tsukasa', 'Miyuki']
      end
    end
  end

end


describe 'Kwery::QueryExecutor#transaction' do

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


describe 'Kwery::QueryExecutor#delete' do

  it "deletes data specified by id" do
    miyuki = q.get('members') {|c| c.where('name', 'Miyuki') }
    q.delete('members', miyuki['id'])
    q.get('members') {|c| c.where('name', 'Miyuki') }.should == nil
    q.get_all('members').length.should == 6
  end

  it "deletes data specified by block" do
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

  it "can take Model class" do
    q.get_all(Member).length == 2
    q.delete(Member) {|c| c.where(:name, 'ryouou') }
    q.get_all(Member).length == 1
    q.get_all(Member).first['name'] == 'sos'
  end

  it "can take model object" do
    members = q.get_all(Member)
    members.length.should == 3
    q.delete(members.first)
    q.get_all(Member).length.should == 2
  end

end


describe "Kwery::Qwery#delete_all" do

  it "deletes all data" do
    q.get_all('members').length.should > 0
    q.delete_all('members')
    q.get_all('members').length.should == 0
  end

  it "can take Model class" do
    q.get_all('teams').length.should > 0
    q.delete_all(Team)
    q.get_all('teams').length.should == 0
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
