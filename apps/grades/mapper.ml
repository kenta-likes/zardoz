open Util;;
let (key, value) = Program.get_input() in
let lst = (List.rev_map split_spaces (split_to_class_lst value)) in 
let extract acc elm = 
  match elm with
  | k::score::[] -> (k, score)::acc
  | _ -> failwith "Why" in
Program.set_output (List.fold_left extract [] lst)
