let unpack : string -> int*int = Util.unmarshal in
let (key, values) = Program.get_input() in
let rawVals = List.rev_map (fun elem -> unpack elem) values in
let compare (bid1, _) (bid2,_) = if bid1 < bid2 then 1
                else if bid1 > bid2 then -1 else 0 in
let sortedVals = List.rev_map (fun elem -> snd elem) (List.sort compare rawVals) in
let sum = List.fold_left (fun acc v ->
    if v = 0 then
        0
    else
        acc + v )
    0 sortedVals in
    Program.set_output [string_of_int sum]