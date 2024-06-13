require 'etc'
require 'msgpack'

RubyVM::YJIT.enable

def process_chunk(chunk)
  city_map = Hash::new { |hash, key| hash[key] = { min: 0, max: 0, sum: 0, count: 0} }
  chunk.each_line do |line|
    city, temp = line.split(";")
    temp = temp.to_f
    city_map[city][:max] = temp if temp > city_map[city][:max]
    city_map[city][:min] = temp if temp < city_map[city][:min]
    city_map[city][:sum] += temp
    city_map[city][:count] += 1
  end
  city_map.to_msgpack
end


CityMap = Hash::new { |hash, key| hash[key] = DataStruct.new(0, 0, 0, 0) }
DataStruct = Struct.new(:min, :max, :sum, :count)
start_execution = Time.now

# process the file in chunks
FILE_SIZE = File.size("measurements.txt")
CPU_COUNT = 8
CHUNK_SIZE = (FILE_SIZE / CPU_COUNT) + 1
pointer = 0
child_processes = Hash::new
chunk_count = 0
while pointer < FILE_SIZE && chunk_count < CPU_COUNT
  remaining_size = FILE_SIZE - pointer
  chunk_size = [CHUNK_SIZE, remaining_size].min
  chunk = IO.read("measurements.txt", chunk_size, pointer)
  chunk_count += 1
  break if chunk.nil?
  last_line_delimiter = chunk.rindex("\n")
  processed_chunk = chunk[0..last_line_delimiter]
  reader, writer = IO.pipe
  pid = fork do
      reader.close
      city_hash = process_chunk(processed_chunk)
      writer.write(city_hash)
      writer.close
    end
  writer.close
  child_processes[pid] = reader
  pointer += last_line_delimiter + 1
end

while ready = IO.select(child_processes.values)
  ready[0].each do |reader|
    city_map = nil
    u = MessagePack::Unpacker.new(reader)
    u.each { |obj| city_map = obj }
    city_map.each do |city, data|
      CityMap[city].max = [CityMap[city].max, data["max"]].max
      CityMap[city].min = [CityMap[city].min, data["min"]].min
      CityMap[city].sum += data["sum"]
      CityMap[city].count += data["count"]
    end
    pid = child_processes.key(reader)
    child_processes.delete(pid) if pid
    Process.wait(pid)
  end
  break if child_processes.empty?
end

reader.close

puts "Execution time: %.2f min" % ((Time.now - start_execution) / 60.0)


