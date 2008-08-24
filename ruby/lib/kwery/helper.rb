###
### $Rev$
### $Release: $
### $Copyright$
### $License$
###

module ::Enumerable

  def index_by(key=nil)
    hash = {}
    if key
      self.each do |item|
        hash[item[key]] = item
      end
    else
      self.each do |item|
        hash[yield(item)] = item
      end
    end
    return hash
  end

  def group_by(key=nil)
    hash = {}
    if key
      self.each do |item|
        (hash[item[key]] ||= []) << item
      end
    else
      self.each do |item|
        (hash[yield(item)] ||= []) << item
      end
    end
    return hash
  end

  def collect_by_key(key)
    return self.collect {|item| item[key] }
  end

  def apply(*args)
    return self.collect {|item| item.__send__(*args) }
  end

  def to_hash
    hash = {}
    self.each {|key, val| hash[key] = val }
    return hash
  end

end


class ::Hash

  def slice(*keys)
    hash = {}
    keys.each {|key| hash[key] = self[key] }
    return hash
  end

end
