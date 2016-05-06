
hash = Hash.new { |hash, key| hash[key] = 0 }

line_counter = 0
File.foreach("part-r-00000") do |line|
	line_counter += 1
	hash[line.split(":")] += 1
end

puts "#{line_counter} : #{hash.keys.count}"

