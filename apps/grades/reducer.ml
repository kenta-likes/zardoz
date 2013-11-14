open Util;;
let (key, values) = Program.get_input() in
let bf v1 v2 :int = 
  let f1 = float_of_string v1 in let f2 = float_of_string v2 in
  if f1 > f2 then 1 else if f1 = f2 then 0 else -1 in
let sorted = List.sort bf values in 
let size   = List.length sorted  in
  if size mod 2 = 1 then Program.set_output (List.nth (size/2))
  else
  let g1 = float_of_string (List.nth (size/2)) in 
  let g2 = float_of_string (List.nth (size/2 + 1)) in 
  Program.set_output [string_of_float ((g1 +. g2) /. 2.)]

