open Util
open Worker_manager

(* TODO implement these *)
let map (kv_pairs: (string * string) list) (map_filename:string) : (string * string) list = 
	(* ? Is the kv_pairs the complete list from the inputfile? or partial ?
	 * 1. Initialize a mapper worker by calling Worker_manager.initialize_mappers *)
    let wm = Worker_manager.initialize_mappers map_filename in
	(* 2. Sends individual unmapped (id, body) pairs tp available mappers *)
	let rec helper pairs acc = 
        match pairs with
        (* Base case. Return the result*)
        | [] -> acc@l
        (* more tasks to do *)
        | (k, v)::tl -> 
            let kevin = Worker_manager.pop_worker wm in (* Kevin the mapper! *)
            begin
            	match Worker_manager.map kevin k v with (* Kevin goes to work*)
            	| Some l -> (* success. push Kevin and move onto next task *) 
            	    Worker_manager.push_worker wm kevin ; helper tl (acc@l)
            	| None   -> (* dead kevin?? *)   helper pairs acc
            end
        in
        helper kv_pairs []
	(* *)
  failwith "Go back whence you came! Trouble the soul of my Mother no more!"

let combine (kv_pairs: (string * string) list) : (string * string list) list = 
  failwith "You have been doomed ever since you lost the ability to love."

let reduce (kvs_pairs : (string * string list) list) (reduce_filename : string) : (string * string list) list =
  failwith "The only thing necessary for evil to triumph is for good men to do nothing"

let map_reduce app_name mapper_name reducer_name kv_pairs =
  let map_filename    = Printf.sprintf "apps/%s/%s.ml" app_name mapper_name  in
  let reduce_filename = Printf.sprintf "apps/%s/%s.ml" app_name reducer_name in
  let mapped   = map kv_pairs map_filename in
  let combined = combine mapped in
  let reduced  = reduce combined reduce_filename in
  reduced

