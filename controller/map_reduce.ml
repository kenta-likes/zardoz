open Util
open Worker_manager


module T = Thread_pool
module H = Hashtbl
module WM = Worker_manager 
module M = Mutex
let mutex_map = M.create () 
let mutex_red = M.create ()
exception ManagerClosed


(* because default sleep function won't let me sleep for less than a second *)
let zzz (sec: float) = ignore (Unix.select [] [] [] sec)

(* Helper function that adds everything in kvlst to a hashtable. *)
let rec makehash (acc: ('a, 'b) H.t) kvlst =
  match kvlst with
  | (k,v)::t -> (H.add acc k v); makehash acc t 
  | []   -> acc


(* define job for mapper *)
<<<<<<< HEAD
let mapperjob (wm1: (WM.mapper WM.worker_manager)) k v todo results ()  =
  (* pop a worker *)
  try begin
=======
let mapperjob (wm1: (WM.mapper WM.worker_manager)) k v todo results () : unit =
  (* pop a worker *) try 
>>>>>>> 096c0ee231e1adb6dde34dd9755e9075d411c22d
  let kevin  = WM.pop_worker wm1 in
  (* do work *)
  let result :'a list option = WM.map kevin k v in 
  match result with
    | Some res -> (* Success *) M.lock mutex_map ;
      if (H.mem todo k) then begin 
        (* update todo and results and push a worker *) 
        (H.remove todo k); (results := (res@ !results));
        M.unlock mutex_map; WM.push_worker wm1 kevin
      end
      else (* someone else has alreay done this job. *)
        (M.unlock mutex_map; WM.push_worker wm1 kevin)
    | None  -> (* Failure *) ()
  end
  with _ -> ()

    with _ -> ()


<<<<<<< HEAD
let reducerjob (wm2 : WM.reducer WM.worker_manager) k vlist todo results ()  =
  (* pop a worker *)
  try begin
=======
let reducerjob (wm2 : WM.reducer WM.worker_manager) k vlist todo results () : unit =
  (* pop a worker *) try
>>>>>>> 096c0ee231e1adb6dde34dd9755e9075d411c22d
  let kevin  = WM.pop_worker wm2 in
  let result :'a list option = WM.reduce kevin k vlist in
  match result  with
  | Some res -> (*sucess*) M.lock mutex_red;
    if (H.mem todo k) then begin 
        (* update todo and results, and push a worker *) 
        (H.remove todo k); (results := ((k,res) :: !results)); 
        M.unlock mutex_red; WM.push_worker wm2 kevin
      end
    else (M.unlock mutex_red; WM.push_worker wm2 kevin)
  | None -> ()
<<<<<<< HEAD
  end
=======
  
>>>>>>> 096c0ee231e1adb6dde34dd9755e9075d411c22d
  with _ -> ()


(**************************** Main Functions ****************************)

let map kv_pairs map_filename : (string * string) list = 
  (*1. create thread pool *)
  let tpool = T.create 50 in
  (*2. create a list that stores the result. thread-safe *)
  let results = ref [] in 
  (*3. create a hashtbl that stores unfinished tasks. thread-safe *)
  let todo = makehash (H.create (2 * (List.length kv_pairs)+1)) kv_pairs in
  (*4. initialize worker manager *)
  let wm1 = WM.initialize_mappers map_filename in  
  let rec helper () : (string * string) list = 
    (*5. Assign jobs *)
    let f k v = ignore(T.add_work (mapperjob wm1 k v todo results) tpool) in 
    begin ignore(H.iter f todo);
    (*6. Sleep for 0.1 seconds *) zzz 1.;
    (*7. Check for undone jobs *)
    Mutex.lock mutex_map; 
    if (H.length todo) = 0 then 
      (WM.clean_up_workers wm1; T.destroy tpool; 
      Mutex.unlock mutex_map; !results)
      else (Mutex.unlock mutex_map; helper ())
    end 
  in
  helper ()



(* Combine values with the same keys. This function won't preserve order. *)
let combine (kv_pairs: (string * string) list) : (string * string list) list = 
  let hash_in = makehash (H.create 113) kv_pairs in 
  let hash_out = H.create 113 in 
  let helper k v =
    if (H.mem hash_out k) then H.replace hash_out k (v::(H.find hash_out k))
    else H.add hash_out k (v::[]) 
  in H.iter helper hash_in; 
  H.fold (fun k v acc -> (k,v)::acc) hash_out []
  



let reduce kvs_pairs reduce_filename : (string * string list) list =
  (*1. create thread pool *)
  let tpool = T.create 50 in
  (*2. create a list that stores the result. thread-safe *)
  let results : (string * string list) list ref = ref [] in 
  (*3. create a hashtbl that stores unfinished tasks. thread-safe *)
  let todo = makehash (H.create (2 * (List.length kvs_pairs)+1)) kvs_pairs in
  (*4. initialize worker manager *)
  let wm2 = WM.initialize_reducers reduce_filename in 
  let rec helper () : (string * string list) list = 
    (*5. Assign jobs *)
    let f k v = ignore(T.add_work (reducerjob wm2 k v todo results) tpool) in 
    begin ignore(H.iter f todo);
    (*6. Sleep for 0.1 seconds *) zzz 1.;
    (*7. Check for unfinished jobs *)
    Mutex.lock mutex_red; 
    if (H.length todo) = 0 
    then (WM.clean_up_workers wm2; T.destroy tpool; Mutex.unlock mutex_red; 
      !results) 
    else (Mutex.unlock mutex_red; helper ())
    end in
  helper ()


let map_reduce app_name mapper_name reducer_name kv_pairs =
  let map_filename    = Printf.sprintf "apps/%s/%s.ml" app_name mapper_name  in
  let reduce_filename = Printf.sprintf "apps/%s/%s.ml" app_name reducer_name in
  let mapped   = map kv_pairs map_filename in
  let combined = combine mapped in
  let reduced  = reduce combined reduce_filename in
  reduced

