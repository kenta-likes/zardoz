open Util
open Worker_manager


module T = Thread_pool
module H = Hashtbl
module WM = Worker_manager 
module M = Mutex
type worker_type = Map | Reduce 
type worker   =  MapperKevin of WM.mapper | ReducerKevin of WM.reducer
let mutex_map = M.create () 
let mutex_red = M.create ()


(* because default sleep function won't let me sleep for less than a second *)
let zzz (sec: float) = ignore (Unix.select [] [] [] sec)

(* Helper function that adds everything in kvlst to a hashtable. *)
let rec makehash (acc: ('a, 'b) H.t) kvlst =
      match kvlst with
      | (k,v)::t -> (H.add acc k v); makehash acc t 
      | []   -> acc


(* send kevin to work. Kevin can be mapper or reducer *)
let sendtowork kevin k v : 'a list option =
  match kevin with
    | MapperKevin  m ->  WM.map m k v  
    | ReducerKevin r ->  WM.reduce r k v


(* define job for mapper *)
let mapperjob wm1 k v results () : unit =
  (* pop a worker *)
  let kevin  = WM.pop_worker wm in
  (* do work *)
  let result :'a list option = WM.map kevin k v in 
  match result with
    | Some res -> (* Success *) M.lock mutex_map ;
      if (H.mem todo k) then begin 
        (* update todo and results and push a worker *) 
        (H.remove todo k); (results := ((k, res) :: !results));
        M.unlock mutex_map; WM.push_worker wm1 kevin
      end
      else (* someone else has alreay done this job. *)
        M.unlock mutex_map
    | None     -> (* Failure, accum. # failure *) ()



let reducerjob wm2 k vlist results () : unit =
  (* pop a worker *)
  let kevin  = WM.pop_worker wm in
  let result :'a list option = WM.reduce kevin k vlist in
  match result  with
  | Some res -> (*sucess*) M.lock mutex_red;
    if (H.mem todo k) then begin 
        (* update todo and results, and push a worker *) 
        (H.remove todo k); (results := ((k, res) :: !results)); 
        M.unlock mutex_red; WM.push_worker wm2 kevin
      end
    else M.unlock mutex_red 
  | None -> ()
    


(* todo  : ('a, 'b) H.t = ('a, 'b) Hashtbl.t
 * wtype : worker_type 
 * wm    : 'a WM.worker_manager *)
let rec mrhelper todo wtype wm = 
  (*1. create thread pool *)
  let tpool = T.create 30 in
  (*2. create a list that stores the result. access should be thread-safe *)
  let results = ref [] in 
  (*3. Assign job *)
  match wm with
  | MapWM wm1 -> 
    let f k v =  T.add_work (mapperjob wm1 k v) tpool in H.iter f todo 
  | RedWM wm2 -> 
    let f k v =  T.add_work (reducerjob wm2 k v) tpool in H.iter f todo    
  (*4. Sleep for 0.1 seconds *) zzz 0.1;
  (*5. Check for undone jobs *)
  if (H.length todo) = 0 then begin 
    (*cleanup workers, and then threadpool.destroy*) 
    match wm with
    | MapWM wm1 -> begin WM.clean_up_workers wm1; T.destroy tp; !results end
    | RedWM wm2 -> begin WM.clean_up_workers wm2; T.destroy tp; !results end
  end
  else (* Not everything is done *)
    mrhelper todo wtype wm





(* TODO implement these *)
let map (kv_pairs: (string * string) list) (map_filename:string) : (string * string) list = 
  (*Create a hashtbl that stores unfinished kv pairs. 
    Access needs to be thread-safe *)
  let unfinished = makehash (H.create (2 * (List.length kv_list))) kv_list in
  mrhelper todo Map (WM.initialize_mappers map_filename)





(* Combine values with the same keys. This function won't preserve order. *)
let combine (kv_pairs: (string * string) list) : (string * string list) list = 
  let rec helper1 acc1 lst1 =
    let helper2 k ini_acc lst2 : string list * (string * string) list = 
      let f (v_lst, carryover) (k1, v1): string list * (string * string) list= 
        (* v_lst : string list , carryover : (string * string) list*)
        if k = k1 then (v1::v_lst, carryover) 
        else (v_lst, (k1, v1)::carryover) in
      List.fold_left f (ini_acc, []) lst2 in
    match lst1 with
    | (k,v)::t -> 
      let (values, carryover) = (helper2 k [v] t) in
      helper1 ((k, values)::acc1) carryover
    | [] -> acc1
  in
  helper1 [] kv_pairs


let reduce (kvs_pairs : (string * string list) list) (reduce_filename : string) : (string * string list) list =
  (*  Create a hashtbl that stores unfinished kv pairs. 
      Access needs to be thread-safe *)
  let todo = makehash (H.create (2 * (List.length kv_list))) kv_list in
  mrhelper todo Reduce (WM.initialize_reducers reduce_filename)



let map_reduce app_name mapper_name reducer_name kv_pairs =
  let map_filename    = Printf.sprintf "apps/%s/%s.ml" app_name mapper_name  in
  let reduce_filename = Printf.sprintf "apps/%s/%s.ml" app_name reducer_name in
  let mapped   = map kv_pairs map_filename in
  let combined = combine mapped in
  let reduced  = reduce combined reduce_filename in
  reduced

