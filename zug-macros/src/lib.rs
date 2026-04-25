use proc_macro::TokenStream;
use proc_macro2::TokenStream as TokenStream2;
use quote::{format_ident, quote};
use syn::{
    parse::Parse, parse::ParseStream, parse_macro_input, Attribute, Data, DeriveInput, Ident,
    LitStr, Token,
};

struct WorkerArgs {
    name: Option<LitStr>,
}

impl Parse for WorkerArgs {
    fn parse(input: ParseStream<'_>) -> syn::Result<Self> {
        let mut name = None;

        while !input.is_empty() {
            let key: syn::Ident = input.parse()?;
            input.parse::<Token![=]>()?;

            match key.to_string().as_str() {
                "name" => {
                    if name.is_some() {
                        return Err(syn::Error::new(key.span(), "duplicate `name`"));
                    }
                    name = Some(input.parse()?);
                }
                _ => return Err(syn::Error::new(key.span(), "expected `name`")),
            }

            if input.is_empty() {
                break;
            }
            input.parse::<Token![,]>()?;
        }

        Ok(Self { name })
    }
}

#[proc_macro_derive(Worker, attributes(zug))]
pub fn derive_worker(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    expand_derive_worker(input).into()
}

fn expand_derive_worker(input: DeriveInput) -> TokenStream2 {
    if !matches!(input.data, Data::Struct(_)) {
        return syn::Error::new_spanned(
            &input.ident,
            "#[derive(zug::Worker)] can only be applied to worker structs",
        )
        .to_compile_error();
    }

    if !input.generics.params.is_empty() {
        return syn::Error::new_spanned(
            &input.generics,
            "#[derive(zug::Worker)] does not support generic worker structs",
        )
        .to_compile_error();
    }

    let worker_args = match parse_zug_attrs(&input.attrs) {
        Ok(worker_args) => worker_args,
        Err(error) => return error.to_compile_error(),
    };

    expand_inferred_registration(&input.ident, worker_args)
}

fn parse_zug_attrs(attrs: &[Attribute]) -> syn::Result<WorkerArgs> {
    let mut name = None;

    for attr in attrs.iter().filter(|attr| attr.path().is_ident("zug")) {
        let worker_args = attr.parse_args::<WorkerArgs>()?;
        if let Some(worker_name) = worker_args.name {
            if name.is_some() {
                return Err(syn::Error::new_spanned(
                    attr,
                    "worker name is configured more than once",
                ));
            }
            name = Some(worker_name);
        }
    }

    Ok(WorkerArgs { name })
}

fn expand_inferred_registration(worker: &Ident, worker_args: WorkerArgs) -> TokenStream2 {
    let class_name_fn = format_ident!("__zug_worker_class_name_{}", worker);
    let default_queue_fn = format_ident!("__zug_worker_default_queue_{}", worker);
    let retry_queue_fn = format_ident!("__zug_worker_retry_queue_{}", worker);
    let register_fn = format_ident!("__zug_worker_register_{}", worker);
    let worker_name = worker_args
        .name
        .map(|name| {
            quote! {
                ::zug::__private::inventory::submit! {
                    ::zug::registry::WorkerAliasRegistration {
                        name: #name,
                        class_name: #class_name_fn,
                    }
                }
            }
        })
        .unwrap_or_default();

    quote! {
        #[allow(non_snake_case)]
        fn #class_name_fn() -> ::std::string::String {
            <#worker as ::zug::Worker<_>>::class_name()
        }

        #[allow(non_snake_case)]
        fn #default_queue_fn() -> ::std::string::String {
            <#worker as ::zug::Worker<_>>::opts()
                .into_opts()
                .queue_name()
                .to_string()
        }

        #[allow(non_snake_case)]
        fn #retry_queue_fn() -> ::std::option::Option<::std::string::String> {
            <#worker as ::zug::Worker<_>>::opts()
                .into_opts()
                .retry_queue_name()
                .map(::std::string::ToString::to_string)
        }

        #[allow(non_snake_case)]
        fn #register_fn(
            processor: &mut ::zug::Processor,
        ) -> ::zug::Result<()> {
            processor.register(<#worker as ::std::default::Default>::default());
            Ok(())
        }

        ::zug::__private::inventory::submit! {
            ::zug::registry::WorkerRegistration {
                class_name: #class_name_fn,
                default_queue: #default_queue_fn,
                retry_queue: #retry_queue_fn,
                register: #register_fn,
            }
        }

        #worker_name
    }
}
