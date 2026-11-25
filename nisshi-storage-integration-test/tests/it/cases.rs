// Copyright (c) Solace Corporation
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! A case runs only if the macro lists it. Each `pub async fn` at the top level of a file of an
//! area is a case, so it must be in that file's case list, and each name in the list must be a
//! case of the file. Any other visible free function in the file, outside a private module, fails
//! the check, so that a case in another form does not go unchecked. A macro in place of an item,
//! at the top level or in an inline module that is not private, fails the check too, because the
//! check does not expand it, so it does not see a case that the macro generates. The check does
//! not examine a method in an `impl`. A caller leaves out an area or a file only if the macro can
//! find its name in a `without` list, so each area and file must have a rule that finds it.

use std::{
    collections::{BTreeMap, BTreeSet},
    fs, io,
    path::{Path, PathBuf},
};

use proc_macro2::{Delimiter, Ident, TokenStream, TokenTree};
use syn::{Item, ItemFn, ItemMacro, ItemMod, Visibility};

/// The source of the macro, which lists every case.
const MACRO: &str = include_str!("../../src/lib.rs");

/// The start of the matcher of the rule in `__unless_listed!` that steps over a name of the
/// `without` list that no rule before it matched. A rule after it never applies.
const STEP_OVER_RULE: &[&str] = &[
    "@", "search", "$", "name", ":", "ident", ";", "[", "$", "other", ":", "ident",
];

/// The items of a case file that the check reads: its visible free functions outside a private
/// module, and the items it cannot see into: a module declared as `mod name;` and a macro. Nothing
/// in a private module is reachable from the macro, so a helper there may be `pub(super)`.
#[derive(Default)]
struct CaseFileItems {
    /// Each `pub async fn` at the top level of the file.
    cases: Vec<String>,
    /// Each other visible free function, at any depth of inline modules that are not private.
    misplaced: Vec<String>,
    /// Each module that the file declares as `mod name;`. The check does not read its functions.
    out_of_line_modules: Vec<String>,
    /// Each macro in place of an item, at the top level or in an inline module that is not
    /// private, as its path and `!`, followed by the name that a `macro_rules!` defines. The
    /// check does not expand a macro, so it does not see the functions that the macro generates.
    macros: Vec<String>,
}

impl CaseFileItems {
    /// Adds the visible free functions and the macros in place of an item of `items`, and of the
    /// inline modules in them that are not private. The walk enters only those modules, so a
    /// method, a private function, and anything in a function body, an `impl`, an initializer or
    /// a private module are never added.
    fn add_items(&mut self, items: &[Item], top_level: bool) {
        for item in items {
            match item {
                Item::Fn(ItemFn { vis, sig, .. }) => match vis {
                    Visibility::Inherited => {}
                    Visibility::Public(_) if top_level && sig.asyncness.is_some() => {
                        self.cases.push(sig.ident.to_string());
                    }
                    _ => self.misplaced.push(sig.ident.to_string()),
                },
                Item::Mod(ItemMod {
                    vis,
                    ident,
                    content,
                    ..
                }) => match content {
                    None => self.out_of_line_modules.push(ident.to_string()),
                    Some((_, items)) if !matches!(vis, Visibility::Inherited) => {
                        self.add_items(items, false);
                    }
                    Some(_) => {}
                },
                Item::Macro(ItemMacro { ident, mac, .. }) => {
                    let segments: Vec<String> = mac
                        .path
                        .segments
                        .iter()
                        .map(|segment| segment.ident.to_string())
                        .collect();
                    let path = segments.join("::");
                    self.macros.push(match ident {
                        Some(defined) => format!("{path}! {defined}"),
                        None => format!("{path}!"),
                    });
                }
                _ => {}
            }
        }
    }
}

/// Returns the items of the Rust source of a case file that the check reads.
fn case_file_items(source: &str) -> syn::Result<CaseFileItems> {
    let mut items = CaseFileItems::default();
    items.add_items(&syn::parse_file(source)?.items, true);
    Ok(items)
}

/// What the macro registers, read from its tokens, so that a comment registers nothing.
struct Registrations {
    /// The case names of each `__file!` list, by area and then by file.
    case_lists: BTreeMap<String, BTreeMap<String, BTreeSet<String>>>,
    /// Each area whose `__area_<area>!` macro `__areas!` calls.
    called_areas: BTreeSet<String>,
    /// Each name that `__unless_listed!` finds in a `without` list: each name whose rule comes
    /// before [`STEP_OVER_RULE`].
    names_that_can_be_left_out: BTreeSet<String>,
}

impl Registrations {
    /// Reads what the macro source registers.
    ///
    /// Returns an error if the source does not parse, if a `__file!` call or an entry of its list
    /// has another form than the macro accepts, or if two `__file!` lists name the same file, in
    /// one area or in two. A file name must be unique across areas, because one `@search` rule in
    /// `__unless_listed!` finds the name for every area.
    fn read(source: &str) -> syn::Result<Self> {
        let mut definitions = BTreeMap::new();
        for item in syn::parse_file(source)?.items {
            if let Item::Macro(ItemMacro {
                ident: Some(name),
                mac,
                ..
            }) = item
            {
                _ = definitions.insert(name.to_string(), mac.tokens);
            }
        }

        let mut case_lists: BTreeMap<String, BTreeMap<String, BTreeSet<String>>> = BTreeMap::new();
        let mut listed_files = BTreeSet::new();
        for call in definitions.values().cloned().flat_map(macro_calls) {
            if call.name != "__file" {
                continue;
            }

            let arguments: Vec<TokenTree> = call.arguments.into_iter().collect();
            let [
                TokenTree::Ident(area),
                _,
                TokenTree::Ident(file),
                _,
                TokenTree::Group(entries),
                ..,
            ] = arguments.as_slice()
            else {
                return Err(syn::Error::new(
                    call.name.span(),
                    "a `__file!` call does not start with `area, file, [cases]`",
                ));
            };

            if !listed_files.insert(file.to_string()) {
                return Err(syn::Error::new(
                    file.span(),
                    format!("two `__file!` lists name the file {file}, in one area or in two"),
                ));
            }

            let names = listed_cases(entries.stream(), file)?;
            _ = case_lists
                .entry(area.to_string())
                .or_default()
                .insert(file.to_string(), names);
        }

        let called_areas = definitions
            .get("__areas")
            .cloned()
            .map(macro_calls)
            .unwrap_or_default()
            .into_iter()
            .filter_map(|call| {
                call.name
                    .to_string()
                    .strip_prefix("__area_")
                    .map(str::to_owned)
            })
            .collect();

        let matchers = definitions
            .get("__unless_listed")
            .cloned()
            .map(rule_matchers)
            .unwrap_or_default();
        let names_that_can_be_left_out = match matchers
            .iter()
            .position(|matcher| starts_with(matcher, STEP_OVER_RULE))
        {
            Some(step_over_rule) => matchers[..step_over_rule]
                .iter()
                .filter_map(|matcher| searched_name(matcher))
                .map(str::to_owned)
                .collect(),
            None => BTreeSet::new(),
        };

        Ok(Self {
            case_lists,
            called_areas,
            names_that_can_be_left_out,
        })
    }

    /// Returns the case names of the `__file!` list of `area`'s file `file`, or `None` if the
    /// macro has no list for the file.
    fn case_list(&self, area: &str, file: &str) -> Option<&BTreeSet<String>> {
        self.case_lists.get(area)?.get(file)
    }
}

/// A call of a macro in the tokens of a macro's definition.
struct MacroCall {
    /// The name of the called macro.
    name: Ident,
    arguments: TokenStream,
}

/// Returns each macro call in `tokens`, at any depth of groups.
fn macro_calls(tokens: TokenStream) -> Vec<MacroCall> {
    let trees: Vec<TokenTree> = tokens.into_iter().collect();
    let mut calls = Vec::new();

    for (index, tree) in trees.iter().enumerate() {
        if let TokenTree::Group(group) = tree {
            calls.extend(macro_calls(group.stream()));
        }

        if let [
            TokenTree::Ident(name),
            TokenTree::Punct(bang),
            TokenTree::Group(arguments),
        ] = &trees[index..(index + 3).min(trees.len())]
            && bang.as_char() == '!'
        {
            calls.push(MacroCall {
                name: name.clone(),
                arguments: arguments.stream(),
            });
        }
    }

    calls
}

/// Returns the case name of each entry of the `__file!` list of `file`.
///
/// An entry is `#[attribute]* [group, ...]? name(arguments)`, so its case name is the identifier
/// directly before its last token, the parenthesised arguments. Returns an error for an entry of
/// another form.
fn listed_cases(entries: TokenStream, file: &Ident) -> syn::Result<BTreeSet<String>> {
    let trees: Vec<TokenTree> = entries.into_iter().collect();
    let mut names = BTreeSet::new();

    for entry in trees.split(is_comma) {
        match entry {
            [] => {}
            [.., TokenTree::Ident(name), TokenTree::Group(arguments)]
                if arguments.delimiter() == Delimiter::Parenthesis =>
            {
                _ = names.insert(name.to_string());
            }
            _ => {
                return Err(syn::Error::new(
                    file.span(),
                    format!(
                        "an entry of the `__file!` list of {file} does not end with \
                         `name(arguments)`"
                    ),
                ));
            }
        }
    }

    Ok(names)
}

/// Returns whether `tree` is a comma.
fn is_comma(tree: &TokenTree) -> bool {
    matches!(tree, TokenTree::Punct(punct) if punct.as_char() == ',')
}

/// Returns the matcher of each rule of a `macro_rules!` definition, as token texts. A rule is
/// `(matcher) => { body };`.
fn rule_matchers(rules: TokenStream) -> Vec<Vec<String>> {
    let trees: Vec<TokenTree> = rules.into_iter().collect();
    trees
        .windows(2)
        .filter_map(|pair| match pair {
            [TokenTree::Group(matcher), TokenTree::Punct(arrow)] if arrow.as_char() == '=' => {
                Some(token_texts(matcher.stream()))
            }
            _ => None,
        })
        .collect()
}

/// Returns the text of each token in `tokens`, with a group written as its opening delimiter, the
/// texts of its tokens and its closing delimiter.
fn token_texts(tokens: TokenStream) -> Vec<String> {
    let mut texts = Vec::new();

    for tree in tokens {
        match tree {
            TokenTree::Group(group) => {
                let (open, close) = match group.delimiter() {
                    Delimiter::Parenthesis => ("(", ")"),
                    Delimiter::Brace => ("{", "}"),
                    Delimiter::Bracket => ("[", "]"),
                    Delimiter::None => ("", ""),
                };
                texts.push(open.to_owned());
                texts.extend(token_texts(group.stream()));
                texts.push(close.to_owned());
            }
            tree => texts.push(tree.to_string()),
        }
    }

    texts
}

/// Returns whether the token texts `texts` start with `prefix`.
fn starts_with(texts: &[String], prefix: &[&str]) -> bool {
    texts.len() >= prefix.len() && texts.iter().zip(prefix).all(|(text, word)| text == word)
}

/// Returns the name that a rule of `__unless_listed!` finds, if its matcher is
/// `@search <name>; [<name> $(...`: the rule matches a `without` list that starts with the name.
fn searched_name(matcher: &[String]) -> Option<&str> {
    let name = matcher.get(2)?.as_str();
    starts_with(matcher, &["@", "search", name, ";", "[", name, "$", "("]).then_some(name)
}

/// Returns the name of each file or directory in `directory`, without its extension.
fn stems(directory: &Path) -> io::Result<Vec<(String, PathBuf)>> {
    let mut stems = Vec::new();
    for entry in fs::read_dir(directory)? {
        let path = entry?.path();
        if let Some(stem) = path.file_stem().and_then(|stem| stem.to_str()) {
            stems.push((stem.to_owned(), path));
        }
    }
    Ok(stems)
}

/// A case that the macro doesn't list in its own file's list runs on no engine, and nothing else
/// warns about it, because an unused public function is not a warning. Each area is a directory
/// in `src`, and each `.rs` file in it holds cases. A visible free function outside a private
/// module that is not a top-level `pub async fn` fails the check, because the check does not count
/// it as a case. An entry of an area that is not a `.rs` file, a `mod name;` in a case file, and a
/// macro in place of an item, at the top level of a case file or in an inline module that is not
/// private, fail the check too, because the check does not read the functions in them or that
/// they generate.
///
/// An area or a file also needs its rule in `__unless_listed!`, or no caller can leave it out.
#[test]
fn every_case_is_registered() -> io::Result<()> {
    let crate_dir = Path::new(env!("CARGO_MANIFEST_DIR"));
    let macro_path = crate_dir.join("src/lib.rs");
    let registrations = Registrations::read(MACRO)
        .map_err(|error| io::Error::other(format!("{}: {error}", macro_path.display())))?;
    let mut unregistered = Vec::new();

    for (area, area_dir) in stems(&crate_dir.join("src"))? {
        if !area_dir.is_dir() {
            continue;
        }

        // The macro expands an area only when `__areas!` calls the area's macro.
        if !registrations.called_areas.contains(&area) {
            unregistered.push(format!(
                "src/{area}: no `__area_{area}!` call in `__areas!`"
            ));
        }

        if !registrations.names_that_can_be_left_out.contains(&area) {
            unregistered.push(format!(
                "src/{area}: no `@search {area};` rule in `__unless_listed!`"
            ));
        }

        for (file, path) in stems(&area_dir)? {
            if path.extension().is_none_or(|extension| extension != "rs") {
                let name = path.file_name().unwrap_or_default().to_string_lossy();
                unregistered.push(format!(
                    "src/{area}/{name}: a case file is a single `.rs` file directly in its area"
                ));
                continue;
            }

            if !registrations.names_that_can_be_left_out.contains(&file) {
                unregistered.push(format!(
                    "src/{area}/{file}.rs: no `@search {file};` rule in `__unless_listed!`"
                ));
            }

            let items = case_file_items(&fs::read_to_string(&path)?)
                .map_err(|error| io::Error::other(format!("{}: {error}", path.display())))?;
            let listed = registrations
                .case_list(&area, &file)
                .cloned()
                .unwrap_or_default();

            for case in &items.cases {
                if !listed.contains(case) {
                    unregistered.push(format!(
                        "src/{area}/{file}.rs: `{case}` is not in the file's `__file!` list"
                    ));
                }
            }

            for function in &items.misplaced {
                unregistered.push(format!(
                    "src/{area}/{file}.rs: `{function}` is visible but is not a top-level `pub \
                     async fn`; make it a case, make it private, or move it to a private module \
                     or to a shared module at the root of src"
                ));
            }

            for module in &items.out_of_line_modules {
                unregistered.push(format!(
                    "src/{area}/{file}.rs: the check does not read the functions of `mod \
                     {module};`; write the module inline in the file"
                ));
            }

            for mac in &items.macros {
                unregistered.push(format!(
                    "src/{area}/{file}.rs: the check does not read the functions that a macro \
                     generates, so it does not check `{mac}`; write each case as a top-level `pub \
                     async fn`, or move the macro to a private module or to a shared module at \
                     the root of src"
                ));
            }

            for name in &listed {
                if !items.cases.contains(name) {
                    unregistered.push(format!(
                        "src/{area}/{file}.rs: the file's `__file!` list names `{name}`, which is \
                         not a case of the file"
                    ));
                }
            }
        }
    }

    assert!(
        unregistered.is_empty(),
        "make each visible free function outside a private module in a file of an area a \
         top-level `pub async fn` case, list each case and nothing else in its file's `__file!` \
         list in the macro in src/lib.rs, and give each area and file a rule in \
         `__unless_listed!`: {unregistered:?}"
    );
    Ok(())
}

/// A small macro in the shapes of the suite's macro, with entries and rules that a comment or a
/// string hides.
const SMALL_MACRO: &str = r#"
macro_rules! __areas {
    ($config:tt) => {
        $crate::__area_listed!($config);
        // $crate::__area_commented_out!($config);
    };
}

macro_rules! __area_listed {
    ($config:tt) => {
        $crate::__file!(listed, file, [
            listed_case(storage),
            // commented_out_case(storage),
            #[ignore = "string_case(storage)"]
            ignored_case(cluster_id, broker_id, storage),
            [not_on_one, not_on_other] grouped_case(broker_id, storage),
        ], $config);
    };
}

macro_rules! __unless_listed {
    ($name:ident; $config:tt; $($item:tt)*) => {};
    (@search early; [early $(, $rest:ident)*]; $($item:tt)*) => {};
    // (@search commented_out; [commented_out $(, $rest:ident)*]; $($item:tt)*) => {};
    (@search $name:ident; [$other:ident $(, $rest:ident)*]; $($item:tt)*) => {
        $crate::__unless_listed!(@search $name; [$($rest),*]; $($item)*);
    };
    (@search late; [late $(, $rest:ident)*]; $($item:tt)*) => {};
    (@search $name:ident; []; $($item:tt)*) => { $($item)* };
}
"#;

#[test]
fn top_level_pub_async_fn_is_a_case() -> syn::Result<()> {
    let items = case_file_items("pub async fn case(storage: S) -> Result<()> { Ok(()) }")?;
    assert_eq!(items.cases, ["case"]);
    assert!(items.misplaced.is_empty());
    Ok(())
}

#[test]
fn case_over_several_lines_with_an_attribute_is_a_case() -> syn::Result<()> {
    let items = case_file_items("#[allow(unused)]\npub\nasync fn case(\n    storage: S,\n) {}")?;
    assert_eq!(items.cases, ["case"]);
    Ok(())
}

#[test]
fn private_async_fn_is_ignored() -> syn::Result<()> {
    let items = case_file_items("async fn helper() {}")?;
    assert!(items.cases.is_empty());
    assert!(items.misplaced.is_empty());
    Ok(())
}

#[test]
fn pub_crate_async_fn_is_misplaced() -> syn::Result<()> {
    let items = case_file_items("pub(crate) async fn case() {}")?;
    assert!(items.cases.is_empty());
    assert_eq!(items.misplaced, ["case"]);
    Ok(())
}

#[test]
fn pub_async_fn_in_a_nested_module_is_misplaced() -> syn::Result<()> {
    let items = case_file_items("pub mod nested { pub async fn case() {} }")?;
    assert!(items.cases.is_empty());
    assert_eq!(items.misplaced, ["case"]);
    Ok(())
}

#[test]
fn function_in_a_private_module_is_ignored() -> syn::Result<()> {
    let items =
        case_file_items("mod private { pub(super) fn helper() {} pub async fn unreachable() {} }")?;
    assert!(items.cases.is_empty());
    assert!(items.misplaced.is_empty());
    Ok(())
}

#[test]
fn out_of_line_module_is_reported() -> syn::Result<()> {
    let items = case_file_items("pub mod outside;")?;
    assert_eq!(items.out_of_line_modules, ["outside"]);
    Ok(())
}

#[test]
fn macro_in_place_of_an_item_is_reported() -> syn::Result<()> {
    let items = case_file_items(
        "macro_rules! case { ($name:ident) => { pub async fn $name() {} }; }\n\
         case!(generated);\n\
         tokio::task_local! { static ID: u32; }\n\
         pub mod nested { inner!(); }",
    )?;
    assert_eq!(
        items.macros,
        ["macro_rules! case", "case!", "tokio::task_local!", "inner!"]
    );
    // The check does not expand `case!`, so it does not see the case that the call generates.
    assert!(items.cases.is_empty());
    Ok(())
}

#[test]
fn macro_in_a_function_or_a_private_module_is_ignored() -> syn::Result<()> {
    let items = case_file_items(
        "pub async fn case() { assert!(true); }\n\
         mod private { macro_rules! helper { () => {}; } helper!(); }",
    )?;
    assert!(items.macros.is_empty());
    Ok(())
}

#[test]
fn macro_in_an_initializer_or_an_impl_is_ignored() -> syn::Result<()> {
    let items = case_file_items(
        "const NAME: &str = concat!(\"a\", \"b\");\n\
         struct Engine; impl Engine { fn helper() { assert!(true); } }",
    )?;
    assert!(items.macros.is_empty());
    Ok(())
}

#[test]
fn top_level_pub_fn_is_misplaced() -> syn::Result<()> {
    let items = case_file_items("pub fn helper() {}")?;
    assert!(items.cases.is_empty());
    assert_eq!(items.misplaced, ["helper"]);
    Ok(())
}

#[test]
fn method_in_an_impl_is_ignored() -> syn::Result<()> {
    let items = case_file_items(
        "struct Engine; impl Engine { pub async fn method(&self) {} fn helper() {} }",
    )?;
    assert!(items.cases.is_empty());
    assert!(items.misplaced.is_empty());
    Ok(())
}

#[test]
fn pub_async_fn_in_a_string_or_a_comment_is_ignored() -> syn::Result<()> {
    let items = case_file_items(
        "const SOURCE: &str = \"\npub async fn in_a_string() {}\n\";\n/*\npub async fn \
         in_a_comment() {}\n*/",
    )?;
    assert!(items.cases.is_empty());
    assert!(items.misplaced.is_empty());
    Ok(())
}

#[test]
fn listed_case_is_registered() -> syn::Result<()> {
    let registrations = Registrations::read(SMALL_MACRO)?;
    assert!(
        registrations
            .case_list("listed", "file")
            .is_some_and(|list| list.contains("listed_case"))
    );
    Ok(())
}

#[test]
fn commented_out_entry_is_not_listed() -> syn::Result<()> {
    let registrations = Registrations::read(SMALL_MACRO)?;
    assert!(
        registrations
            .case_list("listed", "file")
            .is_some_and(|list| !list.contains("commented_out_case"))
    );
    Ok(())
}

#[test]
fn name_in_an_attribute_string_is_not_listed() -> syn::Result<()> {
    let registrations = Registrations::read(SMALL_MACRO)?;
    let list = registrations.case_list("listed", "file");
    // The entry that carries the attribute is listed by its own name.
    assert!(list.is_some_and(|list| list.contains("ignored_case")));
    assert!(list.is_some_and(|list| !list.contains("string_case")));
    Ok(())
}

#[test]
fn entry_with_a_group_is_listed() -> syn::Result<()> {
    let registrations = Registrations::read(SMALL_MACRO)?;
    assert!(
        registrations
            .case_list("listed", "file")
            .is_some_and(|list| list.contains("grouped_case"))
    );
    Ok(())
}

#[test]
fn two_lists_for_one_file_are_an_error() {
    let source = "macro_rules! __area_listed { () => { \
                  __file!(listed, file, [one(storage)], $config); \
                  __file!(listed, file, [other(storage)], $config); } }";
    assert!(Registrations::read(source).is_err());
}

#[test]
fn two_lists_for_one_file_name_in_two_areas_are_an_error() {
    let source = "macro_rules! __area_listed { () => { \
                  __file!(listed, file, [one(storage)], $config); \
                  __file!(other, file, [other(storage)], $config); } }";
    assert!(Registrations::read(source).is_err());
}

#[test]
fn rule_before_the_step_over_rule_lets_a_name_be_left_out() -> syn::Result<()> {
    let registrations = Registrations::read(SMALL_MACRO)?;
    assert!(registrations.names_that_can_be_left_out.contains("early"));
    Ok(())
}

#[test]
fn rule_after_the_step_over_rule_or_commented_out_does_not_count() -> syn::Result<()> {
    let registrations = Registrations::read(SMALL_MACRO)?;
    assert!(!registrations.names_that_can_be_left_out.contains("late"));
    assert!(
        !registrations
            .names_that_can_be_left_out
            .contains("commented_out")
    );
    Ok(())
}

#[test]
fn commented_out_area_call_does_not_count() -> syn::Result<()> {
    let registrations = Registrations::read(SMALL_MACRO)?;
    assert!(registrations.called_areas.contains("listed"));
    assert!(!registrations.called_areas.contains("commented_out"));
    Ok(())
}
