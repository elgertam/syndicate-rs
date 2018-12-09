mod term;
mod bag;
mod skeleton;

use self::term::Term;
use self::skeleton::Index;

// Ord
// Hash

fn main() {
    let capture_label = Term::Symbol("capture".to_string());
    let discard_label = Term::Symbol("discard".to_string());
    let v2 = Term::Double(1234.56);
    let v = Term::Seq(vec![
        Term::Boolean(true),
        Term::Float(123.34),
        v2.clone(),
        Term::SignedInteger(999),
        Term::SignedInteger(-101),
        Term::String("hello".to_string()),
        Term::ByteString("world".as_bytes()),
        Term::Symbol("sym".to_string()),
        Term::Rec(&capture_label, vec![
            Term::Rec(&discard_label, vec![]),
        ]),
    ]);
    let v3 = Term::Double(1234.57);
    println!("v = {:?}", v);
    println!("v[2] = {:?}", v[2]);
    println!("v == v = {:?}", v == v);
    println!("v3 == v = {:?}", v3 == v);
    println!("v[2] == v3 {:?}", v[2] == v3);
    println!("v[2] == v2 {:?}", v[2] == v2);
    println!("v[2] < v3 {:?}", v[2] < v3);
    println!("v[2] < v2 {:?}", v[2] < v2);
    println!("v[2] > v3 {:?}", v[2] > v3);
    println!("v[2] > v2 {:?}", v[2] > v2);
    println!("v[2] <= v3 {:?}", v[2] <= v3);
    println!("v[2] <= v2 {:?}", v[2] <= v2);
    println!("v[2] >= v3 {:?}", v[2] >= v3);
    println!("v[2] >= v2 {:?}", v[2] >= v2);
    println!("v == v3 {:?}", v == v3);
    println!("v == v2 {:?}", v == v2);
    println!("v < v3 {:?}", v < v3);
    println!("v < v2 {:?}", v < v2);
    println!("v > v3 {:?}", v > v3);
    println!("v > v2 {:?}", v > v2);
    println!("v <= v3 {:?}", v <= v3);
    println!("v <= v2 {:?}", v <= v2);
    println!("v >= v3 {:?}", v >= v3);
    println!("v >= v2 {:?}", v >= v2);
    println!("v[8].label() = {:?}", v[8].label());
    println!("v[8].len() = {:?}", v[8].len());
    println!("v[8][0] = {:?}", v[8][0]);
    println!("v[8][0].label() = {:?}", v[8][0].label());
    println!("v[8][0].len() = {:?}", v[8][0].len());
    println!("v.len() = {:?}", v.len());

    let i = Index::new();
}
