MRuby::Gem::Specification.new('mruby-flotte') do |spec|
  spec.license = 'Apache-2'
  spec.author  = 'Hendrik Beskow'
  spec.summary = ''
  spec.add_dependency 'mruby-actor'
  spec.add_dependency 'mruby-lmdb'
  spec.add_dependency 'mruby-simplemsgpack'
  spec.add_dependency 'mruby-statemachine'
  spec.add_dependency 'mruby-libsodium'
  spec.add_dependency 'mruby-method'
end
